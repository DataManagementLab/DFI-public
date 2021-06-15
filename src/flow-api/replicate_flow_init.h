/**
 * @file shuffle_flow_init.h
 * @author cbinnig, lthostrup, tziegler
 * @date 2019-01-30
 */

#pragma once

#include "../utils/Config.h"
#include "schema.h"
#include "../dfi/registry/RegistryClient.h"
#include "../dfi/type/ErrorCodes.h"

enum class DFI_Replicate_flow_optimization {
    Bandwidth,
    BandwidthMulticast, //Note: max tuple size allowed is 4060 bytes (due to rdma UD MTU)
    Latency,
    LatencyMulticast,
    LatencyOrderedMulticast
};

/**
 * @brief 
 * 
 * @param name Name identifier of the flow
 * @param sources Mapping of logical source ids to DFI_Nodes. Attention: only if multicast is used, sources must be identified with an entry in DFI_Nodes, 
                  * if multicast optimization is not chosen, sources can be mapped to NodeID 0.
                  * Note: TargetID's must range from 1 to N, with N being number of targets
 * @param targets Mapping of logical targets to nodes. NodeID is matched to physical node in Config::DFI_NODES. Attention: if multicast optimization is chosen, no DFI_Node have to be referenced in mapping.
                  * Note: TargetID's must range from 1 to N, with N being number of targets
 * @param schema Schema specifying the tuple layout
 * @param opti Optimization goal
 * @param segmentsPerRing Number of segments to initialize the target-side buffers with
 * @return DFI return code
 */
inline int DFI_Replicate_flow_init(string name, std::vector<dfi::source_t> sources, std::vector<dfi::target_t> targets, DFI_Schema schema, DFI_Replicate_flow_optimization opti=DFI_Replicate_flow_optimization::Bandwidth, uint32_t segmentsPerRing = Config::DFI_SEGMENTS_PER_RING)
{
    // std::cout << "DFI_Replicate_flow_init" << std::endl;
    if (name.empty())
    {
        throw invalid_argument("Passed flowname to DFI_replicate_flow_init was empty!");
    }
    dfi::RegistryClient regClient;
    if (targets.empty())
    {
        throw invalid_argument("DFI_replicate_flow_init was passed with not specified targets!");
    }

    for(size_t i = 0; i < targets.size(); i++)
    {
        if (targets[i].nodeId > dfi::Config::DFI_NODES.size())
        {
            Logging::error(__FILE__, __LINE__, "DFI_replicate_flow_init was passed with target " + to_string(targets[i].targetId) + " NodeID " + to_string(targets[i].nodeId) + ", which does not index a node in dfi::Config::DFI_NODES!");
            return DFI_FAILURE;
        }

        //Check for target uniqueness
        for(size_t j = 0; j < targets.size(); j++)
        {
            if (j != i)
                if (targets[i].targetId == targets[j].targetId)
                {
                    Logging::error(__FILE__, __LINE__, "DFI_replicate_flow_init was passed with dublicate target ids! " + to_string(targets[i].targetId));
                    return DFI_FAILURE;
                }
        }
    }

    bool cacheAlign = false;
    dfi::FlowOptimization generic_opti = dfi::FlowOptimization::BW;
    switch (opti)
    {
    case DFI_Replicate_flow_optimization::Bandwidth:
        generic_opti = dfi::FlowOptimization::BW;
        break;
    case DFI_Replicate_flow_optimization::BandwidthMulticast:
        generic_opti = dfi::FlowOptimization::MULTICAST_BW;
        break;
    case DFI_Replicate_flow_optimization::Latency:
        generic_opti = dfi::FlowOptimization::LAT;
        break;
    case DFI_Replicate_flow_optimization::LatencyMulticast:
        generic_opti = dfi::FlowOptimization::MULTICAST;
        break;
    case DFI_Replicate_flow_optimization::LatencyOrderedMulticast:
        generic_opti = dfi::FlowOptimization::MULTICAST_ORDERING;
        break;
    }
    FlowHandle flowHandle(name, sources, targets, schema, UINT32_MAX, FlowType::REPLICATE, generic_opti);
    size_t segmentSize = dfi::Config::DFI_FULL_SEGMENT_SIZE - sizeof(dfi::DFI_SEGMENT_FOOTER_t);
    if (opti != DFI_Replicate_flow_optimization::Bandwidth)
    {
        segmentSize = schema.getTupleSize();
        Logging::info("DFI_replicate_flow_init updated segment sizes to " + to_string(segmentSize) + " b since FlowOptimization is latency");
    }

    if (opti == DFI_Replicate_flow_optimization::LatencyOrderedMulticast)
    {
        if (flowHandle.segmentsPerRing / sources.size() < 8)
        {
            Logging::error(__FILE__, __LINE__, "DFI_replicate_flow_init must have more than 8 segments per source for multicast flow to not drop too many out-of-order packets. Segments per source was: " + to_string(flowHandle.segmentsPerRing / sources.size()) );
            return DFI_FAILURE;
        }
    }

    flowHandle.segmentSizes = segmentSize;
    flowHandle.segmentsPerRing = segmentsPerRing;

    if (!regClient.createFlow(flowHandle, cacheAlign))
        return DFI_FAILURE;
    return DFI_SUCCESS;
};