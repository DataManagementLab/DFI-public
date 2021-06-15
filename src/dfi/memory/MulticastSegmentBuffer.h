#pragma once

#include "../flow/FlowHandle.h"

namespace dfi
{

struct MulticastACK
{
    TargetID target_id;
    seq_no_t seq_no;
    uint16_t budget_increment;
};

class MulticastACKArray
{
    uint8_t *ptr; 
    size_t size; 

public:
    MulticastACKArray() = default;
    MulticastACKArray(MulticastACK *ptr, size_t size) : ptr(reinterpret_cast<uint8_t*>(ptr)), size(size) {}

    //index operator overload to handle UD Header offset
    MulticastACK &operator[] (size_t i)
    {
        uint8_t *ret_ptr = ptr + (sizeof(MulticastACK) + rdma::Config::RDMA_UD_OFFSET) * i + rdma::Config::RDMA_UD_OFFSET;
        if ((size_t)(ret_ptr - ptr) > size)
            throw out_of_range("Out of bounds exception");
        return *reinterpret_cast<MulticastACK*>(ret_ptr);
    }
};
struct MulticastSegmentBuffer
{
    void *buffer;
    size_t segments;
    size_t segment_size;
    size_t current_idx = 0;

    MulticastSegmentBuffer(FlowHandle &flowHandle, size_t segmentSizes) : segments{flowHandle.segmentsPerRing}, segment_size{segmentSizes != 0 ? segmentSizes + sizeof(DFI_MULTICAST_SEGMENT_HEADER_t) : flowHandle.segmentSizes + sizeof(DFI_MULTICAST_SEGMENT_HEADER_t)}
    {
    }

    size_t size()
    {
        return segments * (segment_size + rdma::Config::RDMA_UD_OFFSET);
    }

    // return current segment, increase index
    void *next()
    {
        void *segment = get(current_idx);
        current_idx = ((current_idx + 1) == segments) ? 0 : current_idx + 1;
        return segment;
    }

    // return current segment wihtout setting header, increase index
    void *next_data() {
        return reinterpret_cast<uint8_t *>(next()) + sizeof(DFI_MULTICAST_SEGMENT_HEADER_t);
    }

    // return current segment with set header, increase index
    void *next(seq_no_t segment_idx, bool is_last, SourceID sourceId)
    {
        void *segment = next();
        new (segment) DFI_MULTICAST_SEGMENT_HEADER_t{segment_idx, is_last, sourceId};
        return segment;
    }

    void *get(size_t idx)
    {
        return reinterpret_cast<uint8_t *>(buffer) + idx * (segment_size + rdma::Config::RDMA_UD_OFFSET) + rdma::Config::RDMA_UD_OFFSET;
    }

    void *get_prev()
    {
        const uint32_t prev_idx = (current_idx == 0) ? segments - 1 : current_idx - 1;
        return get(prev_idx);
    }

    DFI_MULTICAST_SEGMENT_HEADER_t *get_header()
    {
        void *segment = get(current_idx);
        return reinterpret_cast<DFI_MULTICAST_SEGMENT_HEADER_t *>(segment);
    }
};
} // namespace dfi