/**
 * @file combiner_flow_init.h
 * @author lthostrup
 * @date 2019-08-01
 */

#pragma once

#include "../utils/Config.h"
#include "schema.h"
#include "../dfi/registry/RegistryClient.h"
#include "../dfi/type/ErrorCodes.h"
#include "../dfi/type/EnumTypes.h"

enum class DFI_Combiner_flow_optimization {
    Bandwidth,
    Latency
};

/**
 * @brief DFI_Combiner_flow_init initializes the combiner flow. The Combiner flow aggregates complete tuples together inside the flow.
 * 
 * @param name - Name identifier of the flow
 * @param sources_count - Number of expected sources to join flow
 * @param targets - Mapping of logical targets to nodes. NodeID is matched to physical node in Config::DFI_NODES. 
                  * Pushed tuples are only aggregated within one target!
                  * Note: TargetID's must range from 1 to N, with N being number of targets
 * @param schema - Schema specifying the tuple layout
 * @param aggrFunc - Enum of aggregation to apply to tuples in flow
 * @param opti - Optimization goal
 * @return int - Return status
 */
inline int DFI_Combiner_flow_init(string name, size_t sources_count, std::vector<target_t> targets, DFI_Schema schema, AggrFunc aggrFunc, DFI_Combiner_flow_optimization opti= DFI_Combiner_flow_optimization::Bandwidth)
{
    dfi::RegistryClient regClient;
    
    if (targets.empty())
    {
        Logging::error(__FILE__, __LINE__, "DFI_Combiner_flow_init was passed with not specified targets!");
        return DFI_FAILURE;
    }

    for(size_t i = 0; i < targets.size(); i++)
    {
        if (targets[i].nodeId > dfi::Config::DFI_NODES.size())
        {
            Logging::error(__FILE__, __LINE__, "DFI_Combiner_flow_init was passed with a target whose NodeID (" + to_string((size_t)targets[i].nodeId) + "), does not index a node in dfi::Config::DFI_NODES!");
            return DFI_FAILURE;
        }
        //Check for target uniqueness
        for(size_t j = 0; j < targets.size(); j++)
        {
            if (j != i)
                if (targets[i].targetId == targets[j].targetId)
                {
                    Logging::error(__FILE__, __LINE__, "DFI_Combiner_flow_init was passed with dublicate target ids! " + to_string(targets[i].targetId));
                    return DFI_FAILURE;
                }
        }
    }

    size_t segmentSize = Config::DFI_FULL_SEGMENT_SIZE - sizeof(DFI_SEGMENT_FOOTER_t);
    if (opti == DFI_Combiner_flow_optimization::Latency)
    {
        segmentSize = schema.getTupleSize();
        Logging::info("DFI_Combiner_flow_init updated segment sizes to " + to_string(segmentSize) + " b since FlowOptimization is latency");
    }
    dfi::FlowOptimization generic_opti = dfi::FlowOptimization::BW;
    switch (opti)
    {
    case DFI_Combiner_flow_optimization::Bandwidth:
        generic_opti = dfi::FlowOptimization::BW;
        break;
    case DFI_Combiner_flow_optimization::Latency:
        generic_opti = dfi::FlowOptimization::LAT;
        break;
    }
    FlowHandle flowHandle(name, targets, schema, sources_count, 0, FlowType::COMBINER, generic_opti);
    flowHandle.segmentSizes = segmentSize;
    flowHandle.aggrFunc = aggrFunc;

    #ifdef FLOWSOURCE_NONTEMP_CACHELINE_STORES_OPTIMIZATION
    bool cacheAlign = true;
    #else
    bool cacheAlign = false;
    #endif
    if (!regClient.createFlow(flowHandle, cacheAlign))
        return DFI_FAILURE;
    else
        return DFI_SUCCESS;
    
};
