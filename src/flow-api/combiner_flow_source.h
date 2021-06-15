/**
 * @file combiner_flow_source.h
 * @author lthostrup
 * @date 2019-08-01
 */

#pragma once

#include "../utils/Config.h"
#include "../dfi/type/Tuple.h"
#include "../dfi/registry/RegistryClient.h"
#include "../dfi/flow/FlowSourceFactory.h"
#include "../dfi/type/ErrorCodes.h"

class DFI_Combiner_flow_source
{
public:
    /**
     * @brief Construct a new dfi shuffle flow source object
     * 
     * @param flowName - Name of flow
     * @param local_node_ids - NodeIDs of local nodes. Optimizes memory access to local memory primitives for local nodes.
     * @param sourceSegmentCount Number of segments to use as output buffer in Source (per target). Tuples will be batched together in segment sizes.
     */
    DFI_Combiner_flow_source(const string &flowName, const size_t sourceSegmentCount = Config::DFI_SOURCE_SEGMENT_COUNT)
    {
        if (flowName.empty())
        {
            throw invalid_argument("Passed flowname to DFI_replicate_flow_source was empty!");
        }
        //Initialize FlowHandle
        dfi::RegistryClient regClient;
        std::unique_ptr<FlowHandle> m_flowHandle = nullptr;
        while ((m_flowHandle = regClient.retrieveFlowHandle(flowName)) == nullptr)
        {
            usleep(Config::DFI_SLEEP_INTERVAL);
        }

        //Sanity checks
        if (m_flowHandle->flowtype != FlowType::COMBINER)
        {
            Logging::error(__FILE__, __LINE__, "DFI_Combiner_flow_source expected a flowhandle with combiner type, but received: " + to_string(m_flowHandle->flowtype));
            return;
        }

        opti = m_flowHandle->optimization;

        m_flowSource = FlowSourceFactory::create(*m_flowHandle, sourceSegmentCount);

        m_targetsCount = m_flowHandle->targets.size();
        m_flowTarget = m_flowHandle->targets[0].targetId;
    }

    ~DFI_Combiner_flow_source()
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

    inline int push(dfi::Tuple const &tuple, TargetID target)
    {
        if(m_flowSource->push(tuple, target)) {
            return DFI_SUCCESS;
        }
        return DFI_FAILURE;
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
    dfi::FlowHandle *m_flowHandle = nullptr;
    std::unique_ptr<dfi::FlowSourceInterface> m_flowSource;
    uint32_t m_keyByteOffset;
    TargetID m_flowTarget;
    FlowOptimization opti;
    size_t m_targetsCount;

    inline TargetID defaultDistFunc(const dfi::Tuple &tuple)
    {
        return ((*(TargetID*)(tuple.getDataPtr() + m_keyByteOffset)) % m_targetsCount) + 1;
    }
};
