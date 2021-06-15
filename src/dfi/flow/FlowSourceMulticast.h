#pragma once

#include "FlowSourceInterface.h"
#include "../memory/BufferWriterMulticast.h"

namespace dfi
{

template <bool ordering>
class FlowSourceMulticast final : public FlowSourceInterface {
    SourceID m_sourceId;
    std::unique_ptr<BufferWriterMulticast<ordering>> m_bufferWriter;
    size_t m_tupleSize = 0;
public:
    FlowSourceMulticast(FlowHandle flowHandle, SourceID sourceId) :
        FlowSourceInterface(flowHandle), m_sourceId{sourceId} {
        
        if (m_flowHandle.optimization != FlowOptimization::MULTICAST && m_flowHandle.optimization != FlowOptimization::MULTICAST_ORDERING) {
            throw new invalid_argument("FlowSourceBW ctor: FlowOptimization not Multicast (FlowOptimization::MULTICAST)");
        }
        
        m_bufferWriter = std::make_unique<BufferWriterMulticast<ordering>>(m_flowHandle, sourceId); 

        m_tupleSize = flowHandle.schema.getTupleSize();
        
        Logging::debug(__FILE__, __LINE__, "BufferWriterMulticast created for flow: " + flowHandle.name);
    }

    ~FlowSourceMulticast() {
        close();
    }
    
    inline bool pushToAll(Tuple const &tuple, bool forceSignaled = false) override
    {
        m_bufferWriter->add(tuple.getDataPtr(), m_tupleSize, forceSignaled);
        return m_bufferWriter->flush(forceSignaled, false);
    }

    inline bool flush(TargetID) override
    {
        //Nothing to do? -> Yes
        return true;
    }

    void close() override
    {
        if (!m_isClosed) {
            m_bufferWriter->close();
        }
        m_isClosed = true;
    }
private:
    // don't call this
    bool push(Tuple const &, TargetID, bool) {
        return false;
    };
};



}