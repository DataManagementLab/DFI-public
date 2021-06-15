#include "FlowTarget.h"

#include "../memory/local_iterators/BufferIteratorMulticast.h"


FlowTarget::FlowTarget(TargetID targetID, FlowHandle flowHandle) : targetID(targetID), m_flowHandle(flowHandle), m_schema(&m_flowHandle.schema)
{
}

FlowTarget::~FlowTarget()
{
    closeFlow();
    if (bufferIterator != nullptr)
    {
        delete bufferIterator;
    }
}

bool FlowTarget::initializeConsume()
{
    if (m_flowActive)
        return false;

    if (m_flowHandle.optimization == FlowOptimization::MULTICAST)
    {
        bufferIterator = new BufferIteratorMulticast<false>{m_flowHandle, this->targetID};
    }
    else if (m_flowHandle.optimization == FlowOptimization::MULTICAST_ORDERING)
    {
        bufferIterator = new BufferIteratorMulticast<true>{m_flowHandle, this->targetID};
    }
    else if (m_flowHandle.optimization == FlowOptimization::MULTICAST_BW)
    {
        size_t max_segment_size = rdma::Config::RDMA_UD_MTU - sizeof(DFI_MULTICAST_SEGMENT_HEADER_t); //MTU minus header size
        size_t max_tuples_in_segment = max_segment_size / m_flowHandle.schema.getTupleSize();

        if (max_tuples_in_segment < 1)
            throw new invalid_argument("FlowSourceMulticastBW ctor: Tuple sizes are bigger than MTU (minus header)!");

        size_t segment_size = max_tuples_in_segment * m_flowHandle.schema.getTupleSize();
        bufferIterator = new BufferIteratorMulticast<false>{m_flowHandle, this->targetID, segment_size};
    }
    else
    {
        RegistryClient regClient;
        auto bufferName = Config::getBufferName(m_flowHandle.name, this->targetID);
        m_bufferHandle = regClient.targetJoinBuffer(bufferName, m_flowHandle.name, this->targetID);
        if (m_bufferHandle == nullptr)
        {
            Logging::debug(__FILE__, __LINE__, "FlowTarget: Didn't find buffer handle for target. Potential error, was flow target initialized with invalid target id? target id: " + to_string(targetID));
            return false;
        }
        bufferIterator = m_bufferHandle->getNewIterator();
    }
    
    m_flowActive = true;

    Logging::debug(__FILE__, __LINE__, "FlowTarget: Initialized consume on flow: " + m_flowHandle.name + ", target: " + to_string(targetID));
    return true;
}

bool FlowTarget::consume(size_t &returnedTuples, Tuple &tuple, bool freeLastTuples)
{
    if (freeLastTuples)
    {
        bufferIterator->free_all_prev_segments();
    }

    if (!m_flowActive)
        return false;

    BufferIterator::HasNextReturn hasNext;
    //Block until some data is received, message lost or flow ended
    while ((hasNext = bufferIterator->has_next()) == BufferIterator::HasNextReturn::FALSE) {}

    if (hasNext == BufferIterator::HasNextReturn::TRUE)
    {
        
        // cout << hasNext << endl;
        size_t size = 0;
        auto dataPtr = bufferIterator->next(size);
        returnedTuples = size / m_schema->getTupleSize();

        tuple.setDataPtr((char*)dataPtr);
        tuple.setSchema(m_schema);
        
        return true;
    }
    else if (hasNext == BufferIterator::LOST)
    {
        returnedTuples = 0;
        return true;
    }

    m_flowActive = false; // All sources have closed flow

    return false;
}


bool FlowTarget::consumeAsync(size_t &returnedTuples, Tuple &tuple, bool freeLastTuples)
{
    (void) freeLastTuples; //todo, if freeLastTuples is false, use a const iterator for the buffer (to be implemented)

    if (!m_flowActive)
        return false;

    BufferIterator::HasNextReturn hasNext;

    hasNext = bufferIterator->has_next();

    if (hasNext == BufferIterator::HasNextReturn::TRUE)
    {
        size_t size = 0;
        auto dataPtr = bufferIterator->next(size);
        // std::cout << "consume - next returned a size of " << size << " sizeof(Tuple): " << sizeof(Tuple) << " flowname: " << m_flowHandle.name << " m_schema->getTupleSize(): " << m_schema->getTupleSize() << std::endl;
        returnedTuples = size / m_schema->getTupleSize();

        tuple.setDataPtr((char*)dataPtr);
        tuple.setSchema(m_schema);
        return true;
    }
    else if (hasNext == BufferIterator::LOST || hasNext == BufferIterator::FALSE)
    {
        returnedTuples = 0;
        return true;
    }

    m_flowActive = false; // All sources have closed flow
    
    return false;
}

void FlowTarget::closeFlow()
{
    if (m_bufferHandle != nullptr)
    {
        NodeID ownNodeID;
        for (size_t i = 0; i < m_flowHandle.targets.size(); i++)
        {
            if (m_flowHandle.targets[i].targetId == targetID)
                ownNodeID = m_flowHandle.targets[i].nodeId;
        }
        
        std::string serverAddr = Config::getIPFromNodeId(ownNodeID);
        rdma::ProtoClient protoClient;
        protoClient.connectProto(serverAddr);

        DFIFreeSegmentsRequest freeSegmentsRequest;
        freeSegmentsRequest.set_name(m_bufferHandle->name);
        Any anyReq;
        Any anyResp;
        anyReq.PackFrom(freeSegmentsRequest);

        protoClient.exchangeProtoMsg(serverAddr, &anyReq, &anyResp);
        
        m_bufferHandle = nullptr;

        if (Config::DFI_CLEANUP_FLOWS)
        {
            RegistryClient regClient;
            regClient.destroyFlow(m_flowHandle);
        }
    }

}



    // std::unique_ptr<BufferHandle> bufferHandle = regClient.retrieveBuffer(m_flowHandle.name);
    // if (bufferHandle != nullptr)
    // {
        
    // }
    // NodeClient nodeClient(0);
    
    // NodeID ownNodeID;
    // for (size_t i = 0; i < m_flowHandle.targets.size(); i++)
    // {
    //     if (m_flowHandle.targets[i].targetId == targetID)
    //         ownNodeID = m_flowHandle.targets[i].nodeId;
    // }
    
    // std::string serverAddr = Config::getIPFromNodeId(ownNodeID);
    // nodeClient.connect(serverAddr, ownNodeID);
    // nodeClient.remoteFree(serverAddr, );

