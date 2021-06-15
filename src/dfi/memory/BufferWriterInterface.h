/**
 * @brief BufferWriter implements two different RDMA strategies to write into the remote memory
 * 
 * @file BufferWriter.h
 * @author cbinnig, lthostrup, tziegler
 * @date 2018-07-06
 */

#pragma once

#include "../../utils/Config.h"
// #if defined(__AVX512F__) && defined(__AVX512BW__)
#ifndef __CUDACC__
#include <immintrin.h>
#endif
namespace dfi
{

struct OutputBuffer
{
    uint8_t* bufferPtr = nullptr;
    size_t segmentCount = 0;
    size_t segmentSize = 0;
    size_t segmentIdx = 0;
    size_t offset = 0; //write offset in current segment
    size_t segmentPadding = 0;
    /**
     * @brief Construct a new Internal Buffer object
     * 
     * @param bufferPtr Pointer to location of output buffer
     * @param segmentCount Number of segments allocated in bufferPtr
     * @param segmentSize Size of segments without footer in bytes
     * @param cacheAlignedSegments Align each segment start to cache lines such that e.g., non_temp operations can be used
     */
    OutputBuffer(void* bufferPtr, size_t segmentCount, size_t segmentSize, bool cacheAlignedSegments) : bufferPtr(reinterpret_cast<uint8_t*>(bufferPtr)), segmentCount(segmentCount), segmentSize(segmentSize)
    {
        assert(segmentSize > 0);
        assert(segmentCount > 0);
        assert(bufferPtr != nullptr);

        if (cacheAlignedSegments)
        {
            size_t cachelineOffset = ((segmentSize + sizeof(DFI_SEGMENT_FOOTER_t)) % Config::CACHELINE_SIZE);
            segmentPadding = Config::CACHELINE_SIZE - (cachelineOffset == 0 ? Config::CACHELINE_SIZE : cachelineOffset);
            // std::cout << "segmentPadding " << segmentPadding << std::endl;
        }
    }
    
    void *next()
    {
        offset = 0;
        if (++segmentIdx == segmentCount)
        {
            segmentIdx = 0;
            // std::cout << "Next, new offset: 0 idx: " << segmentIdx << std::endl;
            return bufferPtr;
        }
        auto a = bufferPtr + (segmentSize + sizeof(DFI_SEGMENT_FOOTER_t) + segmentPadding) * segmentIdx;
        // std::cout << "Next, new offset: " << (segmentSize + sizeof(DFI_SEGMENT_FOOTER_t)) * segmentIdx << " idx: " << segmentIdx << " segmentCount: " << segmentCount << std::endl;
        return a;
    }

    DFI_SEGMENT_FOOTER_t *getFooter()
    {
        // std::cout << "segmentIdx: " << segmentIdx << " footer offset: " << segmentSize + (segmentSize + sizeof(DFI_SEGMENT_FOOTER_t) + segmentPadding) * segmentIdx << std::endl;
        return reinterpret_cast<DFI_SEGMENT_FOOTER_t*>(bufferPtr + segmentSize + (segmentSize + sizeof(DFI_SEGMENT_FOOTER_t) + segmentPadding) * segmentIdx);
    }
};

class BufferWriterInterface
{
public:
    virtual ~BufferWriterInterface() = default;
    virtual bool add(const void *data, size_t size, bool forceSignaled = false) = 0;
    virtual bool flush(bool forceSignaled = false, bool isEndSegment = false) = 0;
    virtual bool close(bool signaled = true) = 0;

protected:
    inline void store_nontemp_64B(const void *dst, const void *src)
    {
        #ifndef __CUDACC__
        __m512i s1 = _mm512_loadu_si512(src);

        _mm512_stream_si512((__m512i*)dst, s1);
        #else
        (void)dst;
        (void)src;
        #endif
    }
};

} // namespace dfi