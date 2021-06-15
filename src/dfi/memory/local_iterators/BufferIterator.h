#pragma once    

#include "../../../utils/Config.h"
#include "../BufferSegment.h"
#include <list>
#include "../../type/EnumTypes.h"

#define NEXT_POW_2(V) \
    do                \
    {                 \
        V--;          \
        V |= V >> 1;  \
        V |= V >> 2;  \
        V |= V >> 4;  \
        V |= V >> 8;  \
        V |= V >> 16; \
        V++;          \
    } while (0)


namespace dfi
{
    
    class BufferIteratorInterface
    {
    public:
        enum HasNextReturn
        {
            TRUE = 0,
            FALSE = 1,
            LOST = 2,
            BUFFER_CLOSED = 3
        };
        
        virtual ~BufferIteratorInterface() = default;
        
        // has_next() checks if a segment is present that can be consumed. All segment-rings in the buffer are checked before returning false
        // returns TRUE if a consumable segment was found
        // returns FALSE if no consumable segment was found in buffer
        // returns BUFFER_CLOSED if the buffer has been closed by all appenders/sources
        virtual HasNextReturn has_next() = 0;
        
        virtual void *next(size_t &ret_size) = 0;
        virtual void free_prev_segments(uint32_t num_segments) = 0;
        virtual void free_all_prev_segments() = 0;
    protected:
        virtual void mark_prev_segment() = 0;
    };
    
    
    
    
    class BufferIterator final : public BufferIteratorInterface
    {
    public:
        BufferIterator(char *rdmaBufferPtr, std::vector<BufferSegment> &entrySegments, ConsumeScheme consumeScheme, size_t ringSize) : consumeScheme(consumeScheme), segmentFreeing(entrySegments.size() * ringSize)
        {
            for (auto &eSeg : entrySegments)
            {
                segment_iterators.emplace_back(eSeg.begin(rdmaBufferPtr, ringSize), eSeg.end(), eSeg.offset - sizeof(uint64_t)); // 3rd argument --> BufferIteratorLat specific
            }
            rings_iter = segment_iterators.begin();
            this->rdmaBufferPtr = rdmaBufferPtr;
        };
            
        HasNextReturn has_next() override
        {
            // std::cout << "has_next called: segment_iterators.size(): " << segment_iterators.size() << std::endl;
            size_t rings_checked = 0;
            size_t rings_to_check = segment_iterators.size();
            
            while (rings_checked < rings_to_check)
            {
                if (rings_iter == segment_iterators.end()){
                    // std::cout << "segment_iterators reached end" << std::endl;
                    rings_iter = segment_iterators.begin();
                }
                // check if previous segment in ring was marked as end
                if (rings_iter->prev->isEndSegment())
                {
                    mark_prev_segment();
                    
                    // remove from segment iterators
                    segment_iterators.erase(rings_iter++);
                    // std::cout << "Buffer Iter: Removing ring since it is marked as end segment. rings: " << segment_iterators.size() << '\n';
                    
                    if (rings_iter == segment_iterators.end())
                        rings_iter = segment_iterators.begin();
                    
                    if (segment_iterators.empty())
                        return HasNextReturn::BUFFER_CLOSED;
                }
                
                if (rings_iter->iter->isConsumable())
                {
                    if (rings_iter->iter->counter == 0 && rings_iter->iter->isEndSegment()) //current segment ring was closed without any data written
                    {
                        segment_iterators.erase(rings_iter++);
                        // std::cout << "segment in ring was marked as end segment, without any data. Removing ring and advancing to next ring... segment_iterators.size() " << segment_iterators.size() << '\n';
                        
                        if (rings_iter == segment_iterators.end())
                            rings_iter = segment_iterators.begin();
                        
                        if (segment_iterators.empty())
                            return HasNextReturn::BUFFER_CLOSED;
                    }
                    else
                    {
                        // std::cout << "Segment was consumable" << std::endl;
                        
                        return HasNextReturn::TRUE;
                    }
                }
                
                if (consumeScheme == ConsumeScheme::ASYNC)
                {
                    //increment counter for iterated rings
                    ++rings_checked;
                    
                    // std::cout << "Advancing to next ring. Checked rings: " << rings_checked << std::endl;
                    //advance iter to next ring
                    ++rings_iter;
                }
            }
            if (segment_iterators.empty())
                return HasNextReturn::BUFFER_CLOSED;
            
            return HasNextReturn::FALSE;
            
        }
        
        void *next(size_t &ret_size) override
        {
            rings_iter->iter->setConsumable(false);
            mark_prev_segment();
            segmentFreeing.addOutstandingSegment(reinterpret_cast<uint64_t*>(rings_iter->counterOffset + rdmaBufferPtr));
            
            void *ret_ptr = rings_iter->iter.getRawData(ret_size);
            
            // std::cout << "Return " << ret_size << " byte" << " - ptr: " << ret_ptr << " counter: " << rings_iter->iter->counter << " ring seg counter offset: " << rings_iter->counterOffset << " seg counter: " << (*(uint64_t*) (rings_iter->counterOffset + rings_iter->iter.m_rdmaBuffer)) << " segment offset: " << rings_iter->iter.current_position << std::endl;
            
            // save current segment as previous
            rings_iter->prev = rings_iter->iter;

            // move to next segment in ring
            // std::cout << "move to next segment in ring" << std::endl;
            rings_iter->iter++;
            
            // proceed to next ring for next call
            // std::cout << "proceed to next ring for next call" << std::endl;
            rings_iter++;
            return ret_ptr;
        }
        
        void free_prev_segments(uint32_t num_segments) override
        {
            segmentFreeing.free(num_segments);
        }

        void free_all_prev_segments() override
        {
            segmentFreeing.freeAll();
        }
        
    protected:
        void mark_prev_segment() override
        {       
            // std::cout << "Iterator mark_prev_segment. prev: " << *rings_iter->prev << std::endl;
            if ((!rings_iter->prev.isDummyStartSegment)) //Do not increment counter if prev segment is not a real segment
            {
                rings_iter->prev->counter = 0;
                rings_iter->prev->setConsumable(false);
                rings_iter->prev->setWriteable(true);
                
                //Increment the consumed segments counter
                // ++(*(uint64_t*) (rings_iter->counterOffset + rdmaBufferPtr));
                // std::cout << "New counter value: " << *(uint64_t*) (rings_iter->counterOffset + rdmaBufferPtr) << " counter offset: " << rings_iter->counterOffset << '\n';
            }
        }

        struct IterHelper
        {
            SegmentIterator iter;
            SegmentIterator end;
            SegmentIterator prev;
            size_t counterOffset;
            IterHelper(SegmentIterator iter, SegmentIterator end, size_t counterOffset = 0) : iter(iter), end(end), prev(end), counterOffset(counterOffset)
            {
                prev->markEndSegment(false);
            };
        };

        struct SegmentFreeing
        {
            SegmentFreeing(size_t size) : size(size)
            {
                outstandingSegments = new uint64_t*[size];
            }

            ~SegmentFreeing()
            {
                delete[] outstandingSegments;
            }

            void free(size_t num)
            {
                for (; num > 0; num--)
                {
                    assert(head != tail);
                    ++(*outstandingSegments[tail]); //Increment credit counter
                    ++tail;
                    if (tail == size)
                        tail = 0;
                }
            }

            void freeAll()
            {
                while (head != ((tail+1)%size) && head != tail)
                {
                        // std::cout << *outstandingSegments[tail] << "("<<tail<<","<<head - tail<<")-";
                    ++(*outstandingSegments[tail]); //Increment credit counter
                    ++tail;
                    if (tail == size)
                        tail = 0;
                }
            }


            void addOutstandingSegment(uint64_t *counter_ptr)
            {
                outstandingSegments[head] = counter_ptr;
                ++head;
                if (head == size) 
                    head = 0;
            }

            uint64_t **outstandingSegments = nullptr;
            size_t head = 0;
            size_t tail = 0;
            const size_t size;
        };
        
        char *rdmaBufferPtr = nullptr;
        std::list<IterHelper> segment_iterators;
        list<IterHelper>::iterator rings_iter; // points to iterator with next data segment
        ConsumeScheme consumeScheme;
        SegmentFreeing segmentFreeing;
    };

} // namespace dfi
