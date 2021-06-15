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

enum class DFI_Shuffle_flow_optimization {
    Bandwidth,
    Latency
};

/**
 * @brief DFI_Shuffle_flow_init initializes the shuffle flow. The shuffle flow shuffles tuples on the specified shuffle key or by providing a shuffling function on push.
 * 
 * @param name - Name identifier of the flow
 * @param sources_count - Number of expected sources to join flow
 * @param targets - Mapping of logical targets to nodes. NodeID is matched to physical node in Config::DFI_NODES. 
                  * Important: TargetID's must be a continuous range from 1 to N, with N being number of targets
 * @param schema - Schema specifying the tuple layout
 * @param shuffle_key_index - Index of the shuffling key. If Flow Sources are provided a distribution function, this parameter will be ignored.
 * @param optimization - Optimization goal
 * @return DFI return code
 */
inline int DFI_Shuffle_flow_init(string name, size_t sources_count, std::vector<dfi::target_t> targets, DFI_Schema schema, size_t shuffle_key_index, 
    DFI_Shuffle_flow_optimization optimization = DFI_Shuffle_flow_optimization::Bandwidth, uint32_t segmentsPerRing = Config::DFI_SEGMENTS_PER_RING, 
    std::vector<TargetOptions> targetSpecificOptions = std::vector<TargetOptions>())
{
    if (shuffle_key_index > schema.getColumnCount() - 1)
    {
        Logging::error(__FILE__, __LINE__, "DFI_Shuffle_flow_init was passed with shuffle_key_index, which exceeds column count in schema!");
        return DFI_FAILURE;
    }

    if (targets.empty())
    {
        return DFI_FAILURE;
        Logging::error(__FILE__, __LINE__, "DFI_Shuffle_flow_init was passed with not specified targets!");
    }
    dfi::RegistryClient regClient;
    
    for(size_t i = 0; i < targets.size(); i++)
    {
        if (targets[i].nodeId > dfi::Config::DFI_NODES.size())
        {
            Logging::error(__FILE__, __LINE__, "DFI_Shuffle_flow_init was passed with target " + to_string(targets[i].targetId) + " NodeID " + to_string(targets[i].nodeId) + ", which does not index a node in dfi::Config::DFI_NODES!");
            return DFI_FAILURE;
        }

        //Check for target uniqueness
        for(size_t j = 0; j < targets.size(); j++)
        {
            if (j != i)
                if (targets[i].targetId == targets[j].targetId)
                {
                    Logging::error(__FILE__, __LINE__, "DFI_Shuffle_flow_init was passed with dublicate target ids! " + to_string(targets[i].targetId));
                    return DFI_FAILURE;
                }
        }
    }
    #ifdef FLOWSOURCE_NONTEMP_CACHELINE_STORES_OPTIMIZATION
    bool cacheAlign = true;
    #else
    bool cacheAlign = false;
    #endif

    size_t segmentSize = dfi::Config::DFI_FULL_SEGMENT_SIZE - sizeof(dfi::DFI_SEGMENT_FOOTER_t);
    if (optimization == DFI_Shuffle_flow_optimization::Latency)
    {
        segmentSize = schema.getTupleSize();
        Logging::info("DFI_Shuffle_flow_init updated segment sizes to " + to_string(segmentSize) + " b since FlowOptimization is latency");
    }

    dfi::FlowOptimization generic_opti = dfi::FlowOptimization::BW;
    switch (optimization)
    {
    case DFI_Shuffle_flow_optimization::Bandwidth:
        generic_opti = dfi::FlowOptimization::BW;
        break;
    case DFI_Shuffle_flow_optimization::Latency:
        generic_opti = dfi::FlowOptimization::LAT;
        break;
    }

    FlowHandle flow_handle(name, targets, schema, sources_count, shuffle_key_index, dfi::FlowType::SHUFFLE, generic_opti);
    flow_handle.segmentSizes = segmentSize;
    flow_handle.segmentsPerRing = segmentsPerRing;
    flow_handle.targetSpecificOptions = targetSpecificOptions;
    if (!regClient.createFlow(flow_handle, cacheAlign))
        return DFI_FAILURE;
    return DFI_SUCCESS;
};
