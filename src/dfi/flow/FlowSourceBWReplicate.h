#pragma once

#include "FlowSourceInterface.h"
#include "../memory/BufferWriterReplicate.h"

namespace dfi
{

class FlowSourceBWReplicate : public FlowSourceInterface {
protected:

    std::shared_ptr<BufferWriterReplicate> m_bufferWriter;
    size_t m_tuplesPushed = 0;
    const size_t m_sourceSegmentCount;
public:
    FlowSourceBWReplicate(FlowHandle flowHandle, size_t sourceSegmentCount)  : 
        FlowSourceInterface{flowHandle}, m_sourceSegmentCount(sourceSegmentCount) {
            
        if (m_sourceSegmentCount == 0) {
            throw new invalid_argument("FlowSourceBWReplicate ctor: m_sourceSegmentCount parameter must be greater than 0");
        }

        if (m_flowHandle.optimization != FlowOptimization::BW) {
            throw new invalid_argument("FlowSourceBWReplicate ctor: FlowOptimization not Bandwidth (FlowOptimization::BW)");
        }

        const size_t totalNodeSize = m_sourceSegmentCount * (flowHandle.segmentSizes + sizeof(DFI_SEGMENT_FOOTER_t)) + Config::CACHELINE_SIZE + sizeof(uint64_t);

        Logging::debug(__FILE__, __LINE__, "FlowSource: Creating NodeClient with size: " + to_string(totalNodeSize));
        auto nodeClient = std::make_shared<NodeClient>(totalNodeSize);

        std::vector<std::string> buffernames;
        for (auto target : m_flowHandle.targets)
        {
            auto bufferName = Config::getBufferName(m_flowHandle.name, target.targetId);
            buffernames.push_back(bufferName);

            Logging::debug(__FILE__, __LINE__, "FlowSourceBWReplicate: Connecting to remote node " + to_string(target.nodeId) + ", and creating BufferWriterReplicate");
            nodeClient->connect(Config::getIPFromNodeId(target.nodeId), target.nodeId);
        }
        m_bufferWriter = make_shared<BufferWriterReplicate>(buffernames, m_registryClient, sourceSegmentCount, nodeClient);
        Logging::debug(__FILE__, __LINE__, "BufferWriterReplicate created");
    }

    ~FlowSourceBWReplicate() {
        if (!m_isClosed) {
            close();
        }
    }

    virtual bool pushToAll(Tuple const &tuple, bool forceSignaled=false) {
        return push(tuple, 0, forceSignaled);
    }

    virtual inline bool push(Tuple const &tuple, TargetID, bool forceSignaled = false) override
    {
        m_bufferWriter->add(tuple.getDataPtr(), m_tupleSize, forceSignaled);
        
        // ++m_tuplesPushed;
        // if (m_tuplesPushed == m_sourceSegmentCount)
        // {
        //     m_tuplesPushed = 0;
        //     return m_bufferWriter->flush(forceSignaled);
        // }
        return true;
    };

    bool flush(TargetID) override
    {
        if (m_tuplesPushed > 0)
        {
            m_bufferWriter->flush();
            m_tuplesPushed = 0;
        }
        return true;
    }

    void close() override
    {
        if (!m_isClosed) {
            // if (m_tuplesPushed > 0)
            // {
            //     std::cout << "Flushing on close " << std::endl;
            //     m_bufferWriter->flush();
            //     m_tuplesPushed = 0;
            // }
            if (!m_bufferWriter->close())
            {
                Logging::error(__FILE__, __LINE__, "Error occured while closing buffer writer");
            }
        }
        m_isClosed = true;
    }
};
}