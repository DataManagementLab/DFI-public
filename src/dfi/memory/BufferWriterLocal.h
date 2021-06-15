/**
 * @brief BufferWriterLocalBW implements two different RDMA strategies to write into the remote memory
 * 
 * @file BufferWriter.h
 * @author cbinnig, lthostrup, tziegler
 * @date 2018-07-06
 */

#pragma once

#include "../../utils/Config.h"
#include "../../utils/Network.h"
#include "../../rdma-manager/src/rdma/RDMAClient.h"
#include "../registry/RegistryClient.h"

#include "BufferWriterInterface.h"
// #define AVX
#ifdef AVX
#include <immintrin.h>
#endif
namespace dfi
{

class BufferWriterLocalBW final : public BufferWriterInterface
{

  public:
    BufferWriterLocalBW(string& bufferName, RegistryClient *regClient)
    {   
        m_handle = regClient->sourceJoinBuffer(bufferName);
        
        if (m_handle->entrySegments.size() != 1)
        {
            Logging::error(__FILE__, __LINE__, "BufferWriterLocalBW expects only one entry segment in segment ring");
            return;
        }

        m_localBufferSegment = &m_handle->entrySegments.front();
        segmentRingStart = m_localBufferSegment->offset;
        Logging::debug(__FILE__, __LINE__, "BufferWriterLocalBW ctor: offset on entry segment: " + to_string(m_localBufferSegment->offset));

        localRdmaPtr = m_handle->localRdmaPtr;

        m_segmentHeader = new DFI_SEGMENT_FOOTER_t();
        // (DFI_SEGMENT_HEADER_t*)(localRdmaPtr + m_localBufferSegment->offset);

        readHeader(m_localBufferSegment->offset);

        // std::cout << "BufferWriterLocalBW created, offset on entry segment: " << m_localBufferSegment->offset << " nodeid: " << m_handle->node_id << '\n';
    }
    ~BufferWriterLocalBW()
    {
        delete m_segmentHeader;
    };

    //Optimization to write locally with (non-temporal) stores of 64bytes
    bool add_64B(void *data) 
    {
        size_t size = 64;
        if (m_localBufferSegment->size < m_sizeUsed + size)
        {
            // std::cout << "m_localBufferSegment->size " << m_localBufferSegment->size << " m_sizeUsed " << m_sizeUsed << '\n';
            //The data does not fit into segment, split it up --> write first part --> write the second part

            if (m_localBufferSegment->size - m_sizeUsed > 0)
            {
                Logging::error(__FILE__, __LINE__, "Data overlaps two segments, append_64B does not support this. Either use normal append or make segment size divisible by 64B");
                return false;
            }
            getNextSegment();
            m_sizeUsed = 0;

            return add_64B(data);
        }

        #ifdef AVX
        store_nontemp_64B(localRdmaPtr + m_localBufferSegment->offset + m_sizeUsed, data);
        #else
        memcpy(localRdmaPtr + m_localBufferSegment->offset + m_sizeUsed, data, size);
        #endif
        m_sizeUsed = m_sizeUsed + size;
        // std::cout << "m_sizeUsed " << m_sizeUsed << '\n';

        return true;
    };
    bool flush(bool forceSignaled = false, bool isEndSegment = false) override 
    { (void)forceSignaled; (void)isEndSegment; return true; };
    
    bool add(const void *, size_t, bool) override 
    { return true;}

    // //Optimization to write locally with (non-temporal) stores of 64bytes
    // bool append_64B(void *data)
    // {
        
    // }

    //data: ptr to data, size: size in bytes. return: true if successful, false otherwise
    bool append(const void *data, size_t size, bool signaled = false)
    {
        (void)signaled;

        if (m_localBufferSegment->size <= m_sizeUsed + size)
        {
            //The data does not fit into segment, split it up --> write first part --> write the second part
            size_t firstPartSize = m_localBufferSegment->size - m_sizeUsed;
            // Logging::debug(__FILE__, __LINE__, "Data does not fit into segment, firstPartSize: " + to_string(firstPartSize));
            if (firstPartSize > 0)
            {
                writeToSegment(m_localBufferSegment->offset, m_sizeUsed, firstPartSize, data);
            }
            
            m_sizeUsed = m_sizeUsed + firstPartSize;
            if (size - firstPartSize > 0)
            {   //Segment is full --> but there is data to append
                getNextSegment();
                m_sizeUsed = 0;
                return append((void*) ((char*)data + firstPartSize), size - firstPartSize);
            }
            else
            {   //Segment is full --> mark it as consumable
                m_segmentHeader->setConsumable(true);
                m_segmentHeader->setWriteable(false);
                m_segmentHeader->counter = m_sizeUsed;
                writeHeader(m_localBufferSegment->offset);
                return true;
            }
        }

        writeToSegment(m_localBufferSegment->offset, m_sizeUsed, size, data);
        
        m_sizeUsed = m_sizeUsed + size;

        return true;
    }

    bool close(bool signaled = true) override
    {
        (void)signaled;
        m_segmentHeader->counter = m_sizeUsed;
        m_segmentHeader->setWriteable(false);
        m_segmentHeader->setConsumable(true);
        m_segmentHeader->markEndSegment();
        if (m_localBufferSegment == nullptr)
            return true;
        
        writeHeader(m_localBufferSegment->offset);
        return true;
    }

  private:

    #ifdef AVX
    inline void store_nontemp_64B(void *dst, void *src)
    {
        register __m256i *d1 = (__m256i *)dst;
        register __m256i s1 = *((__m256i *)src);
        register __m256i *d2 = d1 + 1;
        register __m256i s2 = *(((__m256i *)src) + 1);

        _mm256_stream_si256(d1, s1);
        _mm256_stream_si256(d2, s2);
    }
    #endif

    inline void __attribute__((always_inline)) writeToSegment(size_t segOffset, size_t insideSegOffset, size_t size, const void *data)
    {
        memcpy(localRdmaPtr + segOffset + insideSegOffset, data, size);
    }

    inline void __attribute__((always_inline)) writeHeader(size_t offset)
    {
        memcpy(localRdmaPtr + offset + m_handle->segmentSizes, m_segmentHeader, sizeof(DFI_SEGMENT_FOOTER_t));
    }

    inline void __attribute__((always_inline)) readHeader(size_t offset)
    {
        memcpy(m_segmentHeader, localRdmaPtr + offset + m_handle->segmentSizes, sizeof(DFI_SEGMENT_FOOTER_t));
    }

            
    bool getNextSegment()
    {
        size_t nextSegmentOffset;
        if (segmentIndex == m_handle->segmentsPerWriter - 1)
        {
            nextSegmentOffset = segmentRingStart;
            segmentIndex = 0;
        }
        else
        {
            nextSegmentOffset = m_localBufferSegment->offset + m_handle->segmentSizes + sizeof(DFI_SEGMENT_FOOTER_t);
            ++segmentIndex;
        }
        // std::cout << "Reading new segment header on offset: " << nextSegmentOffset << '\n';
        // Logging::debug(__FILE__, __LINE__, "BufferWriterLocalBW getting new segment");
        //Set CanConsume flag on old segment
        m_segmentHeader->setConsumable(true);
        m_segmentHeader->setWriteable(false);
        m_segmentHeader->counter = m_sizeUsed;
        writeHeader(m_localBufferSegment->offset);

        //Update m_segmentHeader to header of next segment
        readHeader(nextSegmentOffset);
        // m_rdmaClient->read(m_handle->node_id, nextSegmentOffset, m_segmentHeader, sizeof(DFI_SEGMENT_HEADER_t), true);

        // std::cout << "New segment header, counter: " << m_segmentHeader->counter << ", nextSegOffset: " << m_segmentHeader->nextSegmentOffset << " flags: " << m_segmentHeader->segmentFlags << '\n';
        
        //Check if segment is free
        // size_t retries = 0;
        while (!m_segmentHeader->isWriteable())
        {
            // if (retries >= 128)
            // {
            //     Logging::error(__FILE__, __LINE__, "Maximum retires to get new segment for Local BufferWriter reached!");
            //     return false;
            // }
            // std::cout << "Buffer: " << m_handle->name  << '\n' << "Node id: " << m_handle->node_id  << '\n';
            usleep(10);
            readHeader(nextSegmentOffset);
        }

        //Update segment with new data
        m_localBufferSegment->offset = nextSegmentOffset;
        // m_localBufferSegment->size = m_handle->segmentSizes;
        
        return true;
    
    }

    char* localRdmaPtr = nullptr;
    BufferSegment *m_localBufferSegment = nullptr;
    size_t m_sizeUsed = 0;
    std::unique_ptr<BufferHandle> m_handle;
    DFI_SEGMENT_FOOTER_t* m_segmentHeader;
    size_t segmentIndex = 0;
    size_t segmentRingStart;
};

} // namespace dfi