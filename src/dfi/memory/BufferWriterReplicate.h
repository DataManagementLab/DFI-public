/**
 * @brief BufferWriterReplicate implements a bandwidth optimized RDMA strategy to write one-sided into multible remote memory destinations for replication
 * 
 * @file BufferWriterReplicate.h
 * @author lthostrup
 * @date 2020-04-01
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


class BufferWriterReplicate final: public BufferWriterInterface
{

    struct RemoteBuffer
    {
        size_t curRemoteSegmentOffset = 0;
        size_t lastRemoteSegmentOffset = 0;
        std::unique_ptr<BufferHandle> handle = nullptr;
        DFI_SEGMENT_FOOTER_t* remoteSegFooter;
        size_t sizeUsed = 0;
        size_t remoteSegmentIdx = 0;
        size_t segmentRingStart;
        uint64_t *consumedCnt = 0;
        uint64_t writeableFreeSegments = 0;
    };
  public:
    BufferWriterReplicate(std::vector<std::string> bufferNames, RegistryClient &regClient, size_t outputBufferSegmentCount = Config::DFI_SOURCE_SEGMENT_COUNT, std::shared_ptr<NodeClient> nodeClient = nullptr, bool cacheAlignedSegments = false) : m_nodeClient(nodeClient)
    {
        size_t outputBufferSize = 0;
        for (auto &bufferName : bufferNames)
        {
            RemoteBuffer buffer;
            buffer.handle = regClient.sourceJoinBuffer(bufferName);

            //Assume all buffers have same # of segments
            m_segmentSizes = buffer.handle->segmentSizes;
            size_t outputBufferSize = outputBufferSegmentCount * (m_segmentSizes + sizeof(DFI_SEGMENT_FOOTER_t)) + Config::CACHELINE_SIZE;
            if (m_nodeClient == nullptr)
            {
                m_nodeClient = std::make_shared<NodeClient>(outputBufferSize + bufferNames.size() * (sizeof(DFI_SEGMENT_FOOTER_t) + sizeof(uint64_t)));
            }

            m_nodeClient->connect(Config::getIPFromNodeId(buffer.handle->node_id), buffer.handle->node_id);

            if (buffer.handle->entrySegments.size() != 1)
            {
                Logging::error(__FILE__, __LINE__, "BufferWriterReplicate expects only one entry segment in segment ring");
                return;
            }

            buffer.curRemoteSegmentOffset = buffer.handle->entrySegments.front().offset;
            buffer.lastRemoteSegmentOffset = buffer.curRemoteSegmentOffset;
            buffer.segmentRingStart = buffer.curRemoteSegmentOffset;
            buffer.remoteSegFooter = (DFI_SEGMENT_FOOTER_t *)m_nodeClient->localAlloc(sizeof(DFI_SEGMENT_FOOTER_t));
            buffer.consumedCnt = (uint64_t*)m_nodeClient->localAlloc(sizeof(uint64_t));
            readFooterFromRemote(buffer, buffer.curRemoteSegmentOffset); 
            readCounterFromRemote(buffer);
            buffer.writeableFreeSegments = *buffer.consumedCnt;

            buffers.push_back(std::move(buffer));
        }

        // std::cout << "Allocating for segment header..." << '\n';

        //Read header
        // std::cout << "Reading remote header..." << '\n';

        void *outputBuffer = m_nodeClient->localAlloc(outputBufferSize);
        if (outputBuffer == nullptr)
        {
            Logging::error(__FILE__, __LINE__, "BufferWriterReplicate could not allocate space for internal buffer!");
            return;
        }

        void* alignedOutputBuffer = outputBuffer;
        size_t alignedOutputBufferSize = outputBufferSize;
        //offset segment offset so it is data portion is cache aligned
        std::align(Config::CACHELINE_SIZE, outputBufferSize - Config::CACHELINE_SIZE, alignedOutputBuffer, alignedOutputBufferSize);

        // std::cout << "Allocating internal buffer... outputBuffer was: " << outputBuffer << " is: " << alignedOutputBuffer << " size was: " << internalBufferSize << " is: " << alignedOutputBufferSize << '\n';
        m_outputBuffer = new OutputBuffer(alignedOutputBuffer, outputBufferSegmentCount, m_segmentSizes, cacheAlignedSegments);
        m_curSegment = alignedOutputBuffer;
        // std::cout << "BufferWriter created, offset on entry segment: " << m_curRemoteSegmentOffset << " nodeid: " << m_handle->node_id << '\n';
    }
    ~BufferWriterReplicate()
    {
        delete m_outputBuffer;
    }

    inline bool add(const void *data, size_t size, bool forceSignaled = false) override
    {
        memcpy(reinterpret_cast<uint8_t*>(m_curSegment) + m_outputBuffer->offset, data, size);

        m_outputBuffer->offset += size;
        if (m_outputBuffer->offset == m_segmentSizes)
        {
            flush(forceSignaled);
        }
        return true;
    };

    inline bool flush(bool forceSignaled = false, bool isEndSegment = false) override
    {
        bool success = true;

        auto footer = m_outputBuffer->getFooter();
        footer->setConsumable(true);
        footer->setWriteable(false);
        footer->markEndSegment(isEndSegment);
        footer->counter = m_outputBuffer->offset;

        auto newSegmentPtr = m_outputBuffer->next();
        bool signaled = m_outputBuffer->segmentIdx == 0 || forceSignaled;

        for (auto &buffer : buffers)
        {
            success |= flushSingle(buffer, signaled, isEndSegment);
        }
        
        m_outputBuffer->offset = 0;
        
        m_curSegment = newSegmentPtr;

        return success;
    };



    bool close(bool signaled = true) override
    {
        //Flush if any data is written into local output segment buffer
        if (m_outputBuffer->offset > 0)
        {
            flush(signaled, true);
        }    
        else
        {
            for (auto &buffer : buffers)
            {
                buffer.remoteSegFooter->setWriteable(false);
                buffer.remoteSegFooter->setConsumable(true);
                buffer.remoteSegFooter->markEndSegment();

                //Only update flags on segment footer (offset past counter):
                m_nodeClient->write(buffer.handle->node_id, buffer.lastRemoteSegmentOffset + m_segmentSizes + sizeof(DFI_SEGMENT_FOOTER_t::counter), reinterpret_cast<uint8_t*>(buffer.remoteSegFooter) + sizeof(DFI_SEGMENT_FOOTER_t::counter), sizeof(DFI_SEGMENT_FOOTER_t) - sizeof(DFI_SEGMENT_FOOTER_t::counter), signaled);
            }
        }
        return true;
    }

  private:

    inline bool flushSingle(RemoteBuffer &buffer, bool forceSignaled = false, bool isEndSegment = false)
    {
        //Ensure we don't overwrite non-writeable remote segment (blocking call)
        checkRemoteSegmentWritable(buffer);

        //Write local segment to remote buffer
        m_nodeClient->write(buffer.handle->node_id, buffer.curRemoteSegmentOffset, m_curSegment, m_segmentSizes + sizeof(DFI_SEGMENT_FOOTER_t), forceSignaled);


        //Proceed to next remote segment
        if (likely(!isEndSegment))
            advanceRemoteSegmentRing(buffer);

        return true;
    }


    void advanceRemoteSegmentRing(RemoteBuffer &buffer)
    {
        size_t nextSegmentOffset;
        if (buffer.remoteSegmentIdx == buffer.handle->segmentsPerWriter - 1)
        {
            nextSegmentOffset = buffer.segmentRingStart;
            buffer.remoteSegmentIdx = 0;
        }
        else
        {
            nextSegmentOffset = buffer.curRemoteSegmentOffset + m_segmentSizes + sizeof(DFI_SEGMENT_FOOTER_t);
            ++buffer.remoteSegmentIdx;
        }

        --buffer.writeableFreeSegments;

        //Update segment with new data
        buffer.lastRemoteSegmentOffset = buffer.curRemoteSegmentOffset;
        buffer.curRemoteSegmentOffset = nextSegmentOffset;
    }

    void checkRemoteSegmentWritable(RemoteBuffer &buffer)
    {
        if (buffer.writeableFreeSegments == 0) 
        {
            size_t consumedCntOld = *buffer.consumedCnt; //(outstandingCntRead ? consumedCntOld : *consumedCnt); //If no outstanding read, set old consumed count to current (since no read on counter was done!)
            Logging::debug(__FILE__, __LINE__, "No more segments to write to, reading updated remote counter. consumedCntOld: "+to_string(consumedCntOld)+". Offset: " + to_string(buffer.handle->entrySegments[0].offset - sizeof(uint64_t)));

            while (consumedCntOld == *buffer.consumedCnt)
            {
                readCounterFromRemote(buffer);
            }

            buffer.writeableFreeSegments += *buffer.consumedCnt - consumedCntOld;

            // outstandingCntRead = false;
            Logging::debug(__FILE__, __LINE__, "Read remote counter, new writeableFreeSegments: " + to_string(buffer.writeableFreeSegments));
        }
    }


    inline bool __attribute__((always_inline)) readCounterFromRemote(RemoteBuffer &buffer, bool signaled = true)
    {
        m_nodeClient->read(buffer.handle->node_id, buffer.handle->entrySegments[0].offset - sizeof(uint64_t), buffer.consumedCnt, sizeof(uint64_t), signaled); //Counter is located just before the entrySegment

        return true;
    }

    inline bool readFooterFromRemote(RemoteBuffer &buffer, size_t remoteSegOffset, bool signaled = true)
    {
        m_nodeClient->read(buffer.handle->node_id, remoteSegOffset + m_segmentSizes, buffer.remoteSegFooter, sizeof(DFI_SEGMENT_FOOTER_t), signaled);
        return true;
    }


    std::vector<RemoteBuffer> buffers;
    OutputBuffer *m_outputBuffer = nullptr;
    std::shared_ptr<NodeClient> m_nodeClient;
    void *m_curSegment = nullptr;
    size_t m_segmentSizes;
    uint32_t m_ringSize = 0;
};

} // namespace dfi