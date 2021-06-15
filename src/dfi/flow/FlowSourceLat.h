#pragma once

#include "FlowSourceInterface.h"
#include "../memory/BufferWriter.h"

namespace dfi
{

class FlowSourceLat final : public FlowSourceInterface {
    std::vector<std::unique_ptr<BufferWriter>> m_bufferWriters;
public:
    FlowSourceLat(FlowHandle flowHandle, size_t sourceSegmentCount = Config::DFI_SOURCE_SEGMENT_COUNT)  : 
        FlowSourceInterface(flowHandle) {

        if (m_flowHandle.optimization != FlowOptimization::LAT) {
            throw new invalid_argument("FlowSourceBW ctor: FlowOptimization not Latency (FlowOptimization::LAT)");
        }

        const size_t totalNodeSize = (sourceSegmentCount * (flowHandle.segmentSizes + sizeof(DFI_SEGMENT_FOOTER_t))) * m_flowHandle.targets.size() + (m_flowHandle.targets.size() * (sizeof(DFI_SEGMENT_FOOTER_t) + Config::CACHELINE_SIZE + sizeof(uint64_t)));

        auto nodeClient = std::make_shared<NodeClient>(totalNodeSize);

        const TargetID maxTargetId = std::max_element(m_flowHandle.targets.begin(), m_flowHandle.targets.end())->targetId;
        m_bufferWriters.resize(maxTargetId + 1);

        for (auto target : m_flowHandle.targets)
        {
            auto bufferName = Config::getBufferName(m_flowHandle.name, target.targetId);
            
            Logging::debug(__FILE__, __LINE__, "FlowSourceBW: Connecting to remote node " + to_string(target.nodeId) + ", and creating BufferWriter");
            nodeClient->connect(Config::getIPFromNodeId(target.nodeId), target.nodeId);
        
            // Create BufferWriter
            m_bufferWriters.emplace(m_bufferWriters.begin() + target.targetId, make_unique<BufferWriter>(bufferName, m_registryClient, sourceSegmentCount, nodeClient));

            Logging::debug(__FILE__, __LINE__, "BufferWriter (" + bufferName + ") created for TargetID: " + to_string(target.targetId));
        }
    }

    ~FlowSourceLat() {
        if (!m_isClosed) {
            close();
        }
    }

    inline bool push(Tuple const &tuple, TargetID targetId, bool) override
    {
        auto &bufferWriter = m_bufferWriters[targetId];
        return bufferWriter->add(tuple.getDataPtr(), m_tupleSize);
    };

    inline bool flush(TargetID targetId) override
    {
        //Nothing to do? -> Yes
        (void) targetId;
        return true;
    };

    void close() override
    {
        if (!m_isClosed) {
            for (const auto target : m_flowHandle.targets)
            {
                // std::cout << "Closing target " << target.targetId << std::endl;
                Logging::debug(__FILE__, __LINE__, "Closing BufferWriter on TargetID: " + to_string(target.targetId));
                auto &bufferWriter = m_bufferWriters[target.targetId];
                if (!bufferWriter->close())
                {
                    Logging::error(__FILE__, __LINE__, "Error occured while closing buffer writer for target: " + to_string(target.targetId));
                }
            }
        }
        m_isClosed = true;
    }
};


}