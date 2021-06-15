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

class DFI_Shuffle_flow_source
{   
public:
    /**
     * @brief Construct a new dfi shuffle flow source object
     * 
     * @param flowName Unique flow name identifier
     * @param sourceSegmentCount Number of segments to use as output buffer in Source (per target). Tuples will be batched together in segment sizes.
     * @param nonTempSupport Whether to support non-temporal hints (i.e. usage of pushCacheLineNonTemp). Pointer must be cache-aligned to pushCacheLineNonTemp.
     */
    DFI_Shuffle_flow_source(const string &flowName, const size_t sourceSegmentCount = Config::DFI_SOURCE_SEGMENT_COUNT, bool nonTempSupport = false)
    {
        if (flowName.empty())
        {
            throw invalid_argument("Passed flowname to DFI_replicate_flow_source was empty!");
        }
        dfi::RegistryClient regClient;
        m_flowHandle = std::unique_ptr<FlowHandle>(regClient.retrieveFlowHandle(flowName));
        while(!m_flowHandle)
        {
            m_flowHandle = std::unique_ptr<FlowHandle>(regClient.retrieveFlowHandle(flowName));
            usleep(Config::DFI_SLEEP_INTERVAL);
        }
        if (m_flowHandle->flowtype != FlowType::SHUFFLE)
        {
            Logging::error(__FILE__, __LINE__, "DFI_Shuffle_flow_source expected a flowhandle with shuffle type, but received: " + to_string(m_flowHandle->flowtype));
            return;
        }

        m_flowSource = FlowSourceFactory::create(*m_flowHandle, sourceSegmentCount, 0, nonTempSupport);

        m_keyByteOffset = m_flowHandle->schema.getOffsetForColumn(m_flowHandle->groupKeyIndex);
        m_targetsCount = m_flowHandle->targets.size();
        m_flowTarget = m_flowHandle->targets[0].targetId;        
    }

    ~DFI_Shuffle_flow_source()
    {
        close();
    }
    
    /**
     * @brief Pushes single tuples to target specified with TargetID
     * 
     * @param tuple DFI Tuple type
     * @param target Flow target identifier (match target ids provided in flow initialization)
     * @param forceSignaled Whether or not to enforce blocking behaviour in case this push triggers a flush. Function will in this case block until NIC has carried out the One-sided RDMA write.
     * @return DFI return code
     */
    inline int push(dfi::Tuple const &tuple, TargetID target, bool forceSignaled = false)
    {
        assert(target <= m_flowHandle->targets.size());
        if (!m_flowSource->push(tuple, target, forceSignaled))
            return DFI_FAILURE;
        return DFI_SUCCESS;
    }
    
    
    /**
     * @brief Pushes memory contigous tuples to targetId.
     * pushBulk should be used exclusively (not other push), and with same (or less) tupleCount for each call.
     * pushBulk is only available for Bandwidth optimization!
     * 
     * @param tuple pointer to contigous tuple location. Memory layout must be contigious tuples respecting specified Schema.
     * @param targetId Flow target id
     * @param tupleCount Count of tuples to push into flow. Segment width (DFI_FULL_SEGMENT_SIZE - footer) must be a power of tupleCount size!
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
     * @brief Pushes memory contigous tuples to targetId in size of cachelines. Internal memory copies are done with non-temporal hints!
     * Tuple pointer must be cacheline aligned!
     * pushCacheLineNonTemp should be used exclusively (not other push), and with same (or less) tupleCount for each call.
     * pushCacheLineNonTemp is only available for Bandwidth optimization!
     * 
     * @param tuple pointer to contigous tuple location. Memory layout must be contigious tuples respecting specified Schema.
     * @param targetId Flow target id
     * @param tupleCount Count of tuples to push into flow. Must not be an increasing over function calls.
     * @param forceSignaled Whether or not to enforce blocking behaviour in case this push triggers a flush. Function will in this case block until NIC has carried out the One-sided RDMA write.
     * @return DFI return code
     */
    inline int pushCacheLineNonTemp(void *tuples, TargetID target, bool forceSignaled = false)
    {
        assert(target <= m_flowHandle->targets.size());
        if (!m_flowSource->pushCacheLineNonTemp(tuples, target, forceSignaled))
            return DFI_FAILURE;
        return DFI_SUCCESS;
    }
    
    using f_dist_t = TargetID (*)(const Tuple &);
    /** 
     * @brief Pushes single tuples to flow targets according to the provided distribution function (f_dist_t)
     * 
     * @param tuple DFI Tuple
     * @param f_dist Distribution/routing function to determine tuple to target mapping
     * @param forceSignaled Whether or not to enforce blocking behaviour in case this push triggers a flush. Function will in this case block until NIC has carried out the One-sided RDMA write.
     * @return DFI return code 
     */
    inline int push(dfi::Tuple const &tuple, f_dist_t f_dist, bool forceSignaled = false)
    {
        if (!m_flowSource->push(tuple, f_dist(tuple), forceSignaled))
            return DFI_FAILURE;
        return DFI_SUCCESS;
    }

    /**
     * @brief Pushes single tuples into flow with default distribution function
     * 
     * @param tuple DFI Tuple
     * @param forceSignaled Whether or not to enforce blocking behaviour in case this push triggers a flush. Function will in this case block until NIC has carried out the One-sided RDMA write.
     * @return DFI return code  
     */
    inline int push(dfi::Tuple const &tuple, bool forceSignaled = false)
    {
        // std::cout << "defaultDistFunc to node:" << defaultDistFunc(tuple) << '\n';
        if (m_targetsCount == 1)
        {
            if (!m_flowSource->push(tuple, m_flowTarget, forceSignaled))
                return DFI_FAILURE;
            return DFI_SUCCESS;
        } 
        else
        {
            if (!m_flowSource->push(tuple, defaultDistFunc(tuple), forceSignaled))
                return DFI_FAILURE;
            return DFI_SUCCESS;
        }
    }

    /**
     * @brief Flushes the intermediate output buffer for all targets and makes the tuples pushed until this point consumable for the target.
     * 
     * @return DFI return code
     */
    int flush()
    {
        if (m_flowSource->flushToAll())
            return DFI_SUCCESS;
        return DFI_FAILURE;
    }

    /**
     * @brief Flushes the intermediate output buffer for specified target and makes the tuples pushed until this point consumable for the target.
     * 
     * @param target Flow target identifier
     * @return DFI return code
     */
    int flush(TargetID target)
    {
        if (m_flowSource->flush(target))
            return DFI_SUCCESS;
        return DFI_FAILURE;
    }

    /**
     * @brief Closes the flow from this flow source to indicate no more data is expected
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

private:
    std::unique_ptr<dfi::FlowHandle> m_flowHandle;
    std::unique_ptr<dfi::FlowSourceInterface> m_flowSource;
    uint32_t m_keyByteOffset;
    uint32_t m_targetsCount;
    TargetID m_flowTarget;

    inline TargetID defaultDistFunc(const dfi::Tuple &tuple)
    {
        return ((*(TargetID*)(tuple.getDataPtr() + m_keyByteOffset)) % m_targetsCount) + 1;
    }
};
