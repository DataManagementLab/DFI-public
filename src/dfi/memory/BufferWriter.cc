#include "BufferWriter.h"


#include <immintrin.h>


//BufferWriter must be constructed with cacheAlignedSegments = true, in order to use nontemp!!
bool BufferWriter::add_nontemp64B(void *data, bool forceSignaled = false)
{
    // std::cout << "Adding " << ((size_t*)data)[0] << " to " << m_outputBuffer->offset << '\n';
    auto dst = reinterpret_cast<uint8_t*>(m_curSegment) + m_outputBuffer->offset;
    // __m512i s1 = _mm512_loadu_si512(data);
    _mm512_stream_si512((__m512i*)dst, *(__m512i*)data);
    // memcpy(dst, data, 64);
    m_outputBuffer->offset += 64;
    assert(m_outputBuffer->offset <= m_handle->segmentSizes);

    if (m_outputBuffer->offset == m_handle->segmentSizes)
    {
        flush(forceSignaled);
        // std::cout << "Flushing " << std::endl;
    }
    return true;
};
