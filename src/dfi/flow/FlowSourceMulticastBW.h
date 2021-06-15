#pragma once

#include "FlowSourceInterface.h"
#include "../memory/BufferWriterMulticast.h"

namespace dfi
{

class FlowSourceMulticastBW final : public FlowSourceInterface {
    SourceID m_sourceId;
    std::unique_ptr<BufferWriterMulticast<false>> m_bufferWriter;
    size_t m_tupleSize = 0;
    size_t max_tuples_in_segment;
    size_t cur_tuples_in_segment = 0;
public:
    FlowSourceMulticastBW(FlowHandle flowHandle, SourceID sourceId) :
        FlowSourceInterface(flowHandle), m_sourceId{sourceId} {
        
        if (m_flowHandle.optimization != FlowOptimization::MULTICAST && m_flowHandle.optimization != FlowOptimization::MULTICAST_BW) {
            throw new invalid_argument("FlowSourceMulticastBW ctor: FlowOptimization not MulticastBW (FlowOptimization::MULTICAST)");
        }
        
        size_t max_segment_size = rdma::Config::RDMA_UD_MTU - sizeof(DFI_MULTICAST_SEGMENT_HEADER_t); //MTU minus header size
        max_tuples_in_segment = max_segment_size / m_flowHandle.schema.getTupleSize();

        if (max_tuples_in_segment < 1)
            throw new invalid_argument("FlowSourceMulticastBW ctor: Tuple sizes are bigger than MTU (minus header)!");

        size_t segment_size = max_tuples_in_segment * m_flowHandle.schema.getTupleSize();

        m_bufferWriter = std::make_unique<BufferWriterMulticast<false>>(m_flowHandle, sourceId, segment_size);

        m_tupleSize = flowHandle.schema.getTupleSize();
        
        Logging::debug(__FILE__, __LINE__, "FlowSourceMulticastBW created for flow: " + flowHandle.name);
    }

    ~FlowSourceMulticastBW() {
        close();
    }
    
    inline bool pushToAll(Tuple const &tuple, bool forceSignaled=false) override
    {
        m_bufferWriter->add(tuple.getDataPtr(), m_tupleSize, forceSignaled);
        ++cur_tuples_in_segment;
        if (cur_tuples_in_segment == max_tuples_in_segment)
        {
            m_bufferWriter->flush(forceSignaled);
            cur_tuples_in_segment = 0;
        }
        return true;
    }

    inline bool flush(TargetID) override
    {
        if (cur_tuples_in_segment > 0)
        {
            m_bufferWriter->flush();
            cur_tuples_in_segment = 0;
        }
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