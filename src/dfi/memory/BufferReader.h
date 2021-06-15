/**
 * @brief BufferReader implements a read method that pulls latest bufferhandle from the registry and reads the buffer data
 * 
 * @file BufferReader.h
 * @author cbinnig, lthostrup, tziegler
 * @date 2018-08-13
 */

#pragma once

#include "../../utils/Config.h"
#include "../../utils/Network.h"
#include "../../rdma-manager/src/rdma/ReliableRDMA.h"
#include "../registry/RegistryClient.h"
#include "NodeClient.h"
#include "BufferHandle.h"

namespace dfi
{

class BufferReader
{
public:
    BufferReader(std::unique_ptr<BufferHandle> handle, RegistryClient *regClient, NodeClient*nodeClient = (NodeClient*)nullptr) : m_handle(std::move(handle)), m_regClient(regClient), m_nodeClient(nodeClient)
    {
        if (m_nodeClient == nullptr)
        {
            m_nodeClient = new NodeClient();
            m_nodeClient->connect(Config::getIPFromNodeId(m_handle->node_id), m_handle->node_id);
            deletenodeClient = true;
        }
        
    };
    ~BufferReader()
    {
        if (deletenodeClient)
            delete m_nodeClient;
    };

    void* read(size_t &sizeRead, bool withHeader = false)
    {   
        //Update buffer handle
        std::string flowname = "";
        m_handle = m_regClient->retrieveBuffer(m_handle->name, flowname);
        Logging::debug(__FILE__, __LINE__, "Retrieved new BufferHandle");
        if (m_handle->entrySegments.size() == 0) 
        {
            Logging::error(__FILE__, __LINE__, "Buffer Handle did not contain any segments");
            return nullptr;
        }
        size_t dataSize = 0;
        for (BufferSegment &segment : m_handle->entrySegments)
        {
            dataSize += (segment.size + (withHeader ? sizeof(DFI_SEGMENT_FOOTER_t) : 0)) * m_handle->segmentsPerWriter;
        }
        void* data = m_nodeClient->localAlloc(dataSize);
        DFI_SEGMENT_FOOTER_t* header = (DFI_SEGMENT_FOOTER_t*) m_nodeClient->localAlloc(sizeof(DFI_SEGMENT_FOOTER_t));
        size_t writeoffset = 0;
        for (BufferSegment &segment : m_handle->entrySegments)
        {
            size_t currentSegmentOffset = segment.offset;
            do
            {
                size_t currentHeaderOffset = currentSegmentOffset + segment.size;
                std::cout << "Reading header on offset: " << currentHeaderOffset << '\n';
                m_nodeClient->read(m_handle->node_id, currentHeaderOffset, (void*)(header), sizeof(DFI_SEGMENT_FOOTER_t), true);
                size_t segmentDataSize = header->counter;
                
                //Read data from segment
                if (!withHeader)
                {
                    m_nodeClient->read(m_handle->node_id, currentSegmentOffset, (void*)(((char*)data) + writeoffset), header->counter, true);
                    writeoffset += segmentDataSize;
                }
                else
                {
                    m_nodeClient->read(m_handle->node_id, currentSegmentOffset, (void*)(((char*)data) + writeoffset), header->counter + sizeof(DFI_SEGMENT_FOOTER_t), true);
                    writeoffset += (segmentDataSize + sizeof(DFI_SEGMENT_FOOTER_t));
                }            
                Logging::debug(__FILE__, __LINE__, "Read data from segment");

                currentSegmentOffset = currentSegmentOffset + m_handle->segmentSizes + sizeof(DFI_SEGMENT_FOOTER_t);
            } while (!header->isEndSegment());

            //Read header of segment
            
        }
        sizeRead = writeoffset;
        return (void*) data;
    };

private:
    bool deletenodeClient = false;
    std::unique_ptr<BufferHandle> m_handle = nullptr;
    RegistryClient *m_regClient = nullptr;
    NodeClient*m_nodeClient = nullptr;

};

} // namespace dfi