/**
 * @brief BufferWriter implements an RDMA strategy to write one-sided into the remote memory
 * 
 * @file BufferWriter.h
 * @author cbinnig, lthostrup, tziegler
 * @date 2018-07-06
 */

#pragma once

#include "../../utils/Config.h"
#include "../../utils/Network.h"
#include "NodeClient.h"
#include "../registry/RegistryClient.h"
#include "BufferWriterInterface.h"

#include "BufferHandle.h"
namespace dfi
{

class BufferWriter final : public BufferWriterInterface
{
   
  public:
    BufferWriter() = default;

    BufferWriter(string& bufferName, RegistryClient &regClient, size_t outputBufferSegmentCount = Config::DFI_SOURCE_SEGMENT_COUNT,  std::shared_ptr<NodeClient> nodeClient = nullptr, bool cacheAlignedSegments = false) : m_nodeClient(nodeClient)
    {   
        m_handle = regClient.sourceJoinBuffer(bufferName);
        
        if (m_handle->buffertype != FlowOptimization::LAT && m_handle->buffertype != FlowOptimization::BW)
        {
            Logging::error(__FILE__, __LINE__, "BufferWriter expects the buffer to be Buffertype::LAT or Buffertype::BW)");
            return;
        }

        if (m_handle->entrySegments.size() != 1)
        {
            Logging::error(__FILE__, __LINE__, "BufferWriter expects only one entry segment in segment ring");
            return;
        }

        segmentSizes = m_handle->segmentSizes;
        ringSize = m_handle->segmentsPerWriter;
        auto entrySegment = &m_handle->entrySegments.front();
        currentSegmentOffset = entrySegment->offset;
        lastSegmentOffset = entrySegment->offset;
        nextSegmentOffset = currentSegmentOffset + segmentSizes + sizeof(DFI_SEGMENT_FOOTER_t);
        Logging::debug(__FILE__, __LINE__, "BufferWriter ctor: offset on entry segment: " + to_string(currentSegmentOffset));

        size_t outputBufferSize = outputBufferSegmentCount * (segmentSizes + sizeof(DFI_SEGMENT_FOOTER_t) + (cacheAlignedSegments ? Config::CACHELINE_SIZE : 0)) + Config::CACHELINE_SIZE;

        if (m_nodeClient == nullptr)
        {
            m_nodeClient = std::make_shared<NodeClient>(outputBufferSize + sizeof(DFI_SEGMENT_FOOTER_t) + sizeof(uint64_t)); //Internal buffer, segmentHeader, consumedCnt
            m_nodeClient->connect(Config::getIPFromNodeId(m_handle->node_id), m_handle->node_id);
        }

        m_remoteSegFooter = (DFI_SEGMENT_FOOTER_t *)m_nodeClient->localAlloc(sizeof(DFI_SEGMENT_FOOTER_t));
        consumedCnt = (uint64_t *)m_nodeClient->localAlloc(sizeof(uint64_t));

        void *outputBuffer = m_nodeClient->localAlloc(outputBufferSize);
        if (outputBuffer == nullptr)
        {
            Logging::error(__FILE__, __LINE__, "BufferWriter could not allocate space for internal buffer!");
            return;
        }

        void* alignedOutputBuffer = outputBuffer;
        size_t alignedOutputBufferSize = outputBufferSize;
        //offset segment offset so it is data portion is cache aligned
        std::align(Config::CACHELINE_SIZE, outputBufferSize - Config::CACHELINE_SIZE, alignedOutputBuffer, alignedOutputBufferSize);

        // std::cout << "Allocating internal buffer... outputBuffer was: " << outputBuffer << " is: " << alignedOutputBuffer << " size was: " << outputBufferSegmentCount << " is: " << alignedOutputBufferSize << '\n';
        m_outputBuffer = new OutputBuffer(alignedOutputBuffer, outputBufferSegmentCount, segmentSizes, cacheAlignedSegments);
        m_curSegment = alignedOutputBuffer;

        //Read header
        readFooterFromRemote(); 
        readCounterFromRemote();
        
    }

    ~BufferWriter() = default;


    bool close(bool signaled = false) override
    {
        //Flush if any data is written into local output segment buffer
        if (m_outputBuffer->offset > 0)
        {
            // std::cout << "Closing offset " << m_outputBuffer->offset<< std::endl;
            flush(signaled, true);
        }    
        else
        {
            readLastFooterFromRemote(); // Read counter from last remote segment that is already written
            m_remoteSegFooter->setWriteable(false);
            m_remoteSegFooter->setConsumable(true);
            m_remoteSegFooter->markEndSegment();

            //Don't update counter on footer!
            m_nodeClient->write(m_handle->node_id, 
                lastSegmentOffset + m_handle->segmentSizes + sizeof(DFI_SEGMENT_FOOTER_t::counter), 
                reinterpret_cast<uint8_t*>(m_remoteSegFooter) + sizeof(DFI_SEGMENT_FOOTER_t::counter), 
                sizeof(DFI_SEGMENT_FOOTER_t) - sizeof(DFI_SEGMENT_FOOTER_t::counter), signaled);
        }
        return true;
    }


    //Copies data to internal output segment.
    //Flushes automatically
    inline bool add(const void *data, size_t size, bool forceSignaled = false) override
    {
        auto dst = reinterpret_cast<uint8_t*>(m_curSegment) + m_outputBuffer->offset;
        memcpy(dst, data, size);
        m_outputBuffer->offset += size;
        // std::cout << "Adding " << ((size_t*)data)[0] << " to " << m_outputBuffer->offset << '\n';
        assert(m_outputBuffer->offset <= m_handle->segmentSizes);
        if (m_outputBuffer->offset == m_handle->segmentSizes)// || m_outputBuffer->offset + size > m_handle->segmentSizes)
        {
            flush(forceSignaled);
            // std::cout << "Flushing!" << std::endl;
        }
        return true;
    };

    //BufferWriter must be constructed with cacheAlignedSegments = true, in order to use nontemp!!
    inline bool add_nontemp(void *data, size_t size, bool forceSignaled = false)
    { 
        assert(size % 64 == 0);
        // std::cout << "Adding " << ((size_t*)data)[0] << " to " << m_outputBuffer->offset << '\n';
        for (size_t i = 0; i < size/64; i++)
        {
            auto dst = reinterpret_cast<char *>(m_curSegment) + m_outputBuffer->offset + i*64;
            store_nontemp_64B(dst, data);
        }
        m_outputBuffer->offset += size;
        assert(m_outputBuffer->offset <= m_handle->segmentSizes);

        if (m_outputBuffer->offset == m_handle->segmentSizes)
        {
            flush(forceSignaled);
            // std::cout << "Flushing " << std::endl;
        }
        return true;
    };

    //BufferWriter must be constructed with cacheAlignedSegments = true, in order to use nontemp!!
    bool add_nontemp64B(void *data, bool forceSignaled);

    inline bool flush(bool forceSignaled = false, bool isEndSegment = false) override
    {        
        if (m_outputBuffer->offset == 0)
        {
            std::cout << "Nothing to flush..." << std::endl;
            return true;
        }

        auto footer = m_outputBuffer->getFooter();

        footer->setConsumable(true);
        footer->setWriteable(false);
        footer->markEndSegment(isEndSegment);
        footer->counter = m_outputBuffer->offset;

        auto newSegmentPtr = m_outputBuffer->next();
        bool signaled = m_outputBuffer->segmentIdx == 0 || forceSignaled;

        //Ensure we don't overwrite non-writeable remote segment (blocking call)
        checkRemoteSegmentWritable();

        //Write local segment to remote buffer
        m_nodeClient->write(m_handle->node_id, currentSegmentOffset, m_curSegment, m_handle->segmentSizes + sizeof(DFI_SEGMENT_FOOTER_t), signaled);

        
        m_curSegment = newSegmentPtr;

        //Proceed to next remote segment
        m_outputBuffer->offset = 0;
        
        if (likely(!isEndSegment))
            advanceRemoteSegmentRing();
        ++sent_count;
        return true;
    }
   
    size_t getStallCount() { return stall_count; }
    size_t getSentCount() { return sent_count; }
    double getStallSentRatio() { return 1.0*stall_count/sent_count; }

  private:
    void advanceRemoteSegmentRing()
    {
        if (remoteSegmentIdx == m_handle->segmentsPerWriter - 1)
        {
            nextSegmentOffset = m_handle->entrySegments.front().offset;
            remoteSegmentIdx = 0;
        }
        else
        {
            nextSegmentOffset = currentSegmentOffset + segmentSizes + sizeof(DFI_SEGMENT_FOOTER_t);
            ++remoteSegmentIdx;
        }

        ++writeCnt;
        lastSegmentOffset = currentSegmentOffset;
        currentSegmentOffset = nextSegmentOffset;
    }

    void checkRemoteSegmentWritable()
    {

        //Fetch remote counter BEFORE hitting 0
        //E.g. by half issue read and by 0 check if the *consumedCnt has changed from the old, if yes, calculate new credit, if no, do signaled reads
        if (*consumedCnt - writeCnt == m_handle->segmentsPerWriter/2 || *consumedCnt - writeCnt == m_handle->segmentsPerWriter/4) 
        {
            readCounterFromRemote(false);
        }
            // Logging::debug(__FILE__, __LINE__, "No more segments to write to, reading updated remote counter. consumedCntOld: "+to_string(consumedCntOld)+". Offset: " + to_string(m_handle->entrySegments[0].offset - sizeof(uint64_t)));

        while (*consumedCnt - writeCnt == 0)
        {
            readCounterFromRemote(true);
            ++stall_count;
        }
            
    }

    inline bool __attribute__((always_inline)) readLastFooterFromRemote()
    {
        m_nodeClient->read(m_handle->node_id, lastSegmentOffset + segmentSizes, m_remoteSegFooter, sizeof(DFI_SEGMENT_FOOTER_t), true);
        // std::cout << "Read footer on offset: " << currentSegmentOffset << " footer: " << *m_remoteSegFooter << std::endl;
        return true;
    }

    inline bool __attribute__((always_inline)) readFooterFromRemote()
    {
        m_nodeClient->read(m_handle->node_id, currentSegmentOffset + segmentSizes, m_remoteSegFooter, sizeof(DFI_SEGMENT_FOOTER_t), true);
        // std::cout << "Read footer on offset: " << currentSegmentOffset << " footer: " << *m_remoteSegFooter << std::endl;
        return true;
    }

    inline bool __attribute__((always_inline)) readCounterFromRemote(bool signaled = true)
    {
        m_nodeClient->read(m_handle->node_id, m_handle->entrySegments[0].offset - sizeof(uint64_t), consumedCnt, sizeof(uint64_t), signaled); //Counter is located just before the entrySegment

        return true;
    }

    OutputBuffer *m_outputBuffer = nullptr;
    std::unique_ptr<BufferHandle> m_handle;
    std::shared_ptr<NodeClient> m_nodeClient;
    DFI_SEGMENT_FOOTER_t* m_remoteSegFooter;
    size_t lastSegmentOffset = 0;
    size_t currentSegmentOffset = 0;
    size_t nextSegmentOffset = 0;
    size_t remoteSegmentIdx = 0;
    uint64_t *consumedCnt = 0;
    uint64_t writeCnt = 0;
    uint32_t segmentSizes = 0;
    uint32_t ringSize = 0;
    void *m_curSegment = nullptr;
    size_t stall_count = 0;
    size_t sent_count = 0;
};

} // namespace dfi