#pragma once

#include "FlowSourceInterface.h"
#include "../memory/BufferWriter.h"

namespace dfi
{

class FlowSourceBW : public FlowSourceInterface {
protected:
    struct TargetOutput
    {
        TargetOutput() = default;
        TargetOutput(std::shared_ptr<BufferWriter> bufferWriter) : m_bufferWriter(bufferWriter) {};
        
        std::shared_ptr<BufferWriter> m_bufferWriter;
        size_t m_tuplesPushed = 0;
    };
    std::vector<TargetOutput> m_targetOutputs;
    const size_t m_sourceSegmentCount;
    size_t m_batchTupleCount = 0;
public:
    FlowSourceBW(FlowHandle flowHandle, size_t sourceSegmentCount, size_t nonTempSupport = false)  : 
        FlowSourceInterface{flowHandle}, m_sourceSegmentCount(sourceSegmentCount) {
            
        if (m_sourceSegmentCount == 0) {
            throw new invalid_argument("FlowSourceBW ctor: m_sourceSegmentCount parameter must be greater than 0");
        }

        if (m_flowHandle.optimization != FlowOptimization::BW) {
            throw new invalid_argument("FlowSourceBW ctor: FlowOptimization not Bandwidth (FlowOptimization::BW)");
        }

        const size_t totalNodeSize = (sourceSegmentCount * (flowHandle.segmentSizes + sizeof(DFI_SEGMENT_FOOTER_t) + (nonTempSupport ? Config::CACHELINE_SIZE : 0))) * m_flowHandle.targets.size() + (m_flowHandle.targets.size() * (sizeof(DFI_SEGMENT_FOOTER_t) + Config::CACHELINE_SIZE + sizeof(uint64_t)));
        Logging::debug(__FILE__, __LINE__, "FlowSource: Creating NodeClient with size: " + to_string(totalNodeSize));
        auto nodeClient = std::make_shared<NodeClient>(totalNodeSize);

        const TargetID maxTargetId = std::max_element(m_flowHandle.targets.begin(), m_flowHandle.targets.end())->targetId;
        
        m_targetOutputs.resize(maxTargetId + 1);
        
        for (auto target : m_flowHandle.targets)
        {
            auto bufferName = Config::getBufferName(m_flowHandle.name, target.targetId);
            
            Logging::debug(__FILE__, __LINE__, "FlowSourceBW: Connecting to remote node " + to_string(target.nodeId) + ", and creating BufferWriter");
            nodeClient->connect(Config::getIPFromNodeId(target.nodeId), target.nodeId);
            auto bufferWriter = make_shared<BufferWriter>(bufferName, m_registryClient, m_sourceSegmentCount, nodeClient, nonTempSupport);

            m_targetOutputs.emplace(m_targetOutputs.begin() + target.targetId, bufferWriter);

            Logging::debug(__FILE__, __LINE__, "BufferWriter (" + bufferName + ") created for TargetID: " + to_string(target.targetId));
        }

        m_batchTupleCount = flowHandle.segmentSizes / flowHandle.schema.getTupleSize();
    }

    ~FlowSourceBW() {
        if (!m_isClosed) {
            close();
        }
    }

    virtual inline bool push(Tuple const &tuple, TargetID targetId, bool forceSignaled = false) override
    {
        auto &targetOutput = m_targetOutputs[targetId];
        targetOutput.m_bufferWriter->add(tuple.getDataPtr(), m_tupleSize, forceSignaled);
        
        // ++targetOutput.m_tuplesPushed;
        // if (targetOutput.m_tuplesPushed == m_batchTupleCount)
        // {
        //     targetOutput.m_tuplesPushed = 0;
        //     return targetOutput.m_bufferWriter->flush(forceSignaled);
        // }
        return true;
    };


    /**
     * @brief Pushes memory-contigous tuples to targetId. Tuples are still only flushed when desired sourceSegmentCount is reached.
     * pushBulk should be used exclusively (not other push), and with same tupleCount for each call (except last pushBulk call can be with less tuples)
     * 
     * @param tuple pointer to contigous tuple location. Memory layout must be contigious tuples respecting specified Schema.
     * @param targetId Flow target id
     * @param tupleCount Count of tuples to push into flow
     * @param forceSignaled Whether or not to enforce blocking behaviour in case this push triggers a flush. Function will in this case block until NIC has carried out the One-sided RDMA write.
     * @return true Success
     * @return false Failure
     */
    virtual inline bool pushBulk(void *tuples, TargetID targetId, size_t tupleCount, bool forceSignaled=false) override
    {
        auto &targetOutput = m_targetOutputs[targetId];
        auto bulkSize = m_tupleSize*tupleCount;

        targetOutput.m_bufferWriter->add(tuples, bulkSize, forceSignaled);
        
        // targetOutput.m_tuplesPushed += tupleCount;
        
        // if (targetOutput.m_tuplesPushed + tupleCount > m_batchTupleCount) //Flush tuples if next bulkPush exceeds segment size
        // {
        //     targetOutput.m_tuplesPushed = 0;
        //     std::cout << "Manually flushed!" << std::endl;
        //     std::cout << "Tuple count: " << tupleCount << std::endl;
        //     return targetOutput.m_bufferWriter->flush(forceSignaled);
        // }
        return true;
    }



    /**
     * @brief Pushes memory contigous tuples to targetId in size of cachelines. Internal memory copies are done with non-temporal hints!
     * Tuple pointer must be cacheline aligned!
     * pushCacheLineNonTemp should be used exclusively (not other push), and with same (or less) tupleCount for each call.
     * pushCacheLineNonTemp is only available for Bandwidth optimization!
     * 
     * @param tuple pointer to contigous cache aligned tuple location. Memory layout must be contigious tuples respecting specified Schema.
     * @param targetId Flow target id
     * @param forceSignaled Whether or not to enforce blocking behaviour in case this push triggers a flush. Function will in this case block until NIC has carried out the One-sided RDMA write.
     * @return true Success
     * @return false Failure
     */
    virtual inline bool pushCacheLineNonTemp(void *tuples, TargetID targetId, bool forceSignaled=false) override
    {
        auto &targetOutput = m_targetOutputs[targetId];

        targetOutput.m_bufferWriter->add_nontemp64B(tuples, forceSignaled);
        
        // targetOutput.m_tuplesPushed += tupleCount;
        
        // if (targetOutput.m_tuplesPushed + tupleCount > m_batchTupleCount) //Flush tuples if next bulkPush exceeds segment size
        // {
        //     targetOutput.m_tuplesPushed = 0;
        //     std::cout << "Manually flushed!" << std::endl;
        //     std::cout << "Tuple count: " << tupleCount << std::endl;
        //     return targetOutput.m_bufferWriter->flush(forceSignaled);
        // }
        return true;
    }

    bool flush(TargetID targetId) override
    {
        auto &targetOutput = m_targetOutputs[targetId];
        // if (targetOutput.m_tuplesPushed > 0)
        // {
            targetOutput.m_bufferWriter->flush();
            // targetOutput.m_tuplesPushed = 0;
        // }
        return true;
    }

    void close() override
    {
        if (!m_isClosed) {
            size_t total_stall_count = 0;
            // size_t total_sent_count = 0;
            double stall_sent_ratio = 0;
            for (auto target : m_flowHandle.targets)
            {
                Logging::debug(__FILE__, __LINE__, "Closing BufferWriter on TargetID: " + to_string(target.targetId));
                auto &targetOutput = m_targetOutputs[target.targetId];
                // if (targetOutput.m_tuplesPushed > 0)
                // {
                //     targetOutput.m_bufferWriter->flush();
                //     targetOutput.m_tuplesPushed = 0;
                // }
                if (!targetOutput.m_bufferWriter->close())
                {
                    Logging::error(__FILE__, __LINE__, "Error occured while closing buffer writer for target: " + to_string(target.targetId));
                }
                total_stall_count += targetOutput.m_bufferWriter->getStallCount();
                // total_sent_count += targetOutput.m_bufferWriter->getSentCount();
                stall_sent_ratio += targetOutput.m_bufferWriter->getStallSentRatio();
            }
            // std::cout << "sender_count=" << total_sent_count << std::endl;
            if (Config::PRINT_SOURCE_STATS && total_stall_count > 0)
            {
                std::cout << "sender_stalls=" << total_stall_count << std::endl;
                std::cout << "sender_stall_sent_ratio=" << stall_sent_ratio/m_flowHandle.targets.size() << std::endl;
            }
        }
        m_isClosed = true;
    }
};

class FlowSourceBW_nontemp : public FlowSourceBW {
public:
    FlowSourceBW_nontemp(FlowHandle &flowHandle, size_t sourceSegmentCount)  : FlowSourceBW(flowHandle, sourceSegmentCount, true) {}

    virtual inline bool push(Tuple const &tuple, TargetID targetId, bool forceSignaled = false) override
    {
        auto &targetOutput = m_targetOutputs[targetId];
        targetOutput.m_bufferWriter->add_nontemp(tuple.getDataPtr(), m_tupleSize, forceSignaled);
        
        // ++targetOutput.m_tuplesPushed;
        // if (targetOutput.m_tuplesPushed == m_batchTupleCount)
        // {
        //     targetOutput.m_tuplesPushed = 0;
        //     return targetOutput.m_bufferWriter->flush(forceSignaled);
        // }
        return true;
    };


    /**
     * @brief Pushes memory contigous tuples to targetId. Tuples are still only flushed when desired sourceSegmentCount is reached.
     * pushBulk should be used exclusively (not other push), and with same (or less) tupleCount for each call
     * 
     * @param tuple pointer to contigous tuple location. Memory layout must be contigious tuples respecting specified Schema.
     * @param targetId Flow target id
     * @param tupleCount Count of tuples to push into flow
     * @param forceSignaled Whether or not to enforce blocking behaviour in case this push triggers a flush. Function will in this case block until NIC has carried out the One-sided RDMA write.
     * @return true Success
     * @return false Failure
     */
    virtual inline bool pushBulk(void *tuple, TargetID targetId, size_t tupleCount, bool forceSignaled=false) override
    {
        auto &targetOutput = m_targetOutputs[targetId];
        targetOutput.m_bufferWriter->add_nontemp(tuple, m_tupleSize*tupleCount, forceSignaled);
        
        // targetOutput.m_tuplesPushed += tupleCount;
        
        // if (targetOutput.m_tuplesPushed + tupleCount > m_batchTupleCount) //Flush tuples if next bulkPush exceeds segment size
        // {
        //     targetOutput.m_tuplesPushed = 0;
        //     return targetOutput.m_bufferWriter->flush(forceSignaled);
        // }
        return true;
    }

};
}