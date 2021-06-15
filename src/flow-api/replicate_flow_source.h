/**
 * @file shuffle_flow_source.h
 * @author cbinnig, lthostrup, tziegler
 * @date 2019-01-30
 */

#pragma once

#include "../utils/Config.h"
#include "../dfi/type/Tuple.h"
#include "../dfi/registry/RegistryClient.h"
#include "../dfi/flow/FlowSourceFactory.h"
#include "../dfi/type/ErrorCodes.h"





class DFI_Replicate_flow_source
{   
public:
    
    /**
     * @brief Construct a new dfi replicate flow source object
     * 
     * @param flowName Unique flow name identifier
     * @param sourceId Source identifier - Must correspond to SourceID passed to flow initialization
     */
    DFI_Replicate_flow_source(const string &flowName, SourceID sourceId) : DFI_Replicate_flow_source(flowName, sourceId, Config::DFI_SOURCE_SEGMENT_COUNT) {}

    /**
     * @brief Construct a new dfi replicate flow source object
     * 
     * @param flowName Unique flow name identifier
     * @param sourceId Source identifier - Must correspond to SourceID passed to flow initialization
     * @param sourceSegmentCount Number of segments to use as output buffer in Source (per target). Tuples will be batched together in segment sizes.
     */
    DFI_Replicate_flow_source(const string &flowName, SourceID sourceId, const size_t sourceSegmentCount)
    {
        if (flowName.empty())
        {
            throw invalid_argument("Passed flowname to DFI_replicate_flow_source was empty!");
        }

        dfi::RegistryClient regClient;
        auto m_flowHandle = regClient.retrieveFlowHandle(flowName);
        while(m_flowHandle == nullptr)
        {
            m_flowHandle = regClient.retrieveFlowHandle(flowName);
            usleep(Config::DFI_SLEEP_INTERVAL);
        }
        
        m_flowSource = FlowSourceFactory::create(*m_flowHandle, sourceSegmentCount, sourceId);
        m_targetsCount = m_flowHandle->targets.size();
        for(auto target : m_flowHandle->targets)
        {
            m_flowTargets.push_back(target.targetId);
        }
        opti = m_flowHandle->optimization;
    }

    ~DFI_Replicate_flow_source()  
    {
        close();
        delete m_flowHandle;
    }

    /**
     * @brief Pushes single tuples into flow with default distribution function
     * 
     * @param tuple DFI Tuple
     * @param forceSignaled Whether or not to enforce blocking behaviour in case this push triggers a flush. Function will in this case block until NIC has carried out the One-sided RDMA write.
     * @return DFI return code
     */
    int push(dfi::Tuple const &tuple, bool forceSignaled = false)
    {
        if (!m_flowSource->pushToAll(tuple, forceSignaled)) {
            return DFI_FAILURE;
        }
        return DFI_SUCCESS;
    }

    /**
     * @brief Pushes memory contigous tuples to targetId. Tuples are still only flushed when desired sourceSegmentCount is reached.
     * pushBulk should be used exclusively (not other push), and with same (or less) tupleCount for each call.
     * pushBulk is only available for Bandwidth optimization!
     * 
     * @param tuple pointer to contigous tuple location. Memory layout must be contigious tuples respecting specified Schema.
     * @param targetId Flow target id
     * @param tupleCount Count of tuples to push into flow. Must not be an increasing over function calls.
     * @param forceSignaled Whether or not to enforce blocking behaviour in case this push triggers a flush. Function will in this case block until NIC has carried out the One-sided RDMA write.
     * @return DFI return code
     */
    inline int pushBulk(void *tuples, TargetID target, size_t tupleCount, bool forceSignaled = false)
    {
        assert(target <= m_flowHandle->targets.size());
        if (!m_flowSource->pushBulk(tuples, target, tupleCount, forceSignaled))
            return DFI_FAILURE;
        return DFI_SUCCESS;
    }


    /**
     * @brief Closes the flow from flow source to indicate no more data is expected
     * 
     * @return DFI return code
     */
    int close()
    {
        if (m_flowSource != nullptr)
        {
            m_flowSource->close();
        }
        return DFI_SUCCESS;
    }

    /**
     * @brief Flushes the intermediate output buffer for all targets and makes the tuples pushed until this point consumable for the target.
     * 
     * @return DFI return code
     */
    int flush()
    {
        if (m_flowSource != nullptr)
        {
            m_flowSource->flushToAll();
        }
        return DFI_SUCCESS;
    }
private:
    dfi::FlowHandle *m_flowHandle = nullptr;
    std::unique_ptr<dfi::FlowSourceInterface> m_flowSource;
    uint32_t m_targetsCount;
    vector<TargetID> m_flowTargets;
    FlowOptimization opti;
};
