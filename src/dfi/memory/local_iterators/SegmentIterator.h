/**
 * @file SegmentIterator.h
 * @author cbinnig, lthostrup, tziegler
 * @date 2018-12-10
 */

#pragma once

#include "../../../utils/Config.h"
#include <algorithm>

namespace dfi
{
class SegmentIterator : public std::iterator<
                            std::input_iterator_tag,              // iterator_category
                            DFI_SEGMENT_FOOTER_t,         // value_type
                            DFI_SEGMENT_FOOTER_t,         // difference_type
                            DFI_SEGMENT_FOOTER_t *, // pointer
                            DFI_SEGMENT_FOOTER_t&         // reference
                            >
{

    using header_ptr = DFI_SEGMENT_FOOTER_t *;

  public:
    // empty Iterator
    static SegmentIterator getEndSegmentIterator(){
        SegmentIterator endIter(0, 0, nullptr, 0);
        
        endIter.current_header = new DFI_SEGMENT_FOOTER_t();
        endIter.current_header->markEndSegment();
        endIter.isDummyStartSegment = true;
        endIter.current_position = 0;
        return endIter;
    }

    explicit SegmentIterator(size_t offset, size_t segment_payload_size, char *rdmaBufferPtr, size_t ring_size) : m_rdmaBuffer(rdmaBufferPtr), current_position(offset), m_segment_payload_size(segment_payload_size), m_ring_size(ring_size), m_ring_offset(offset)
    {
        current_header = (header_ptr)(m_rdmaBuffer + offset + segment_payload_size);
        isDummyStartSegment = false;
    };

    // SegmentIterator& operator=(SegmentIterator other){
    //     current_position =  other.current_position;
    //     isDummyStartSegment =  other.isDummyStartSegment;
    //     current_header =  other.current_header;
    //     m_rdmaBuffer =  other.m_rdmaBuffer;
    //     return *this;
    // }



    SegmentIterator &operator++()
    {
        if (m_segment_index == m_ring_size - 1)
        {
            current_position = m_ring_offset;
            m_segment_index = 0;
        }
        else
        {
            current_position = current_position + m_segment_payload_size + sizeof(DFI_SEGMENT_FOOTER_t);
            ++m_segment_index;
        }

        isDummyStartSegment = current_header->isEndSegment();
        current_header = (header_ptr)(m_rdmaBuffer + current_position + m_segment_payload_size);
        return *this;
    }
    
    SegmentIterator operator++(int)
    {
        SegmentIterator retval = *this;
        ++(*this);
        return retval;
    }

    bool operator==(SegmentIterator other) const { return (isDummyStartSegment == other.isDummyStartSegment); }
    bool operator!=(SegmentIterator other) const {return !(*this == other);}
    reference operator*() { return *current_header; }
    pointer operator->() { return current_header;}

    

    void* getRawData(size_t& dataSizeInBytes){
        dataSizeInBytes = current_header->counter;
        return reinterpret_cast<uint8_t*>(current_header) - m_segment_payload_size; // get to raw data by ignoring header
    }


    char *m_rdmaBuffer = nullptr;
//   private:
    size_t current_position = 0;
    bool isDummyStartSegment = false;
    DFI_SEGMENT_FOOTER_t *current_header = nullptr;
    size_t m_segment_payload_size;
    size_t m_ring_size;
    size_t m_segment_index = 0;
    size_t m_ring_offset;
};

} // namespace dfi
