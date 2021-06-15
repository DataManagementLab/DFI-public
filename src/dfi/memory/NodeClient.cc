#include "NodeClient.h"

NodeClient::NodeClient(/* args */){};
NodeClient::NodeClient(size_t memsize) : RDMAClient(memsize){};
NodeClient::~NodeClient(){};

bool NodeClient::remoteAllocSegments(const string &connection, const NodeID nodeID, const string &bufferName, const size_t segmentsCount,
                                     const size_t fullSegmentSize, FlowOptimization buffertype, bool cacheAlign, std::vector<size_t> &offsets, size_t count)
{
    NodeID rdmaNodeID;
    if (!RDMAClient::connect(connection, rdmaNodeID))
    {
        Logging::error(__FILE__, __LINE__, "NodeClient tried to remoteAllocSegments to connection: " + connection + ", but failed to connect.");
        return false;
    }
    DFINodeIDToRDMANodeID[nodeID] = rdmaNodeID;
    if (!rdma::ProtoClient::isConnected(connection))
    {
        Logging::error(__FILE__, __LINE__, "NodeClient tried to remoteAllocSegments to connection: " + connection + ", but connection was not found.");
        return false;
    }

    // std::cout << "RDMAClient::remoteAllocSegments segmentsCount: " << segmentsCount << '\n';
    DFIAllocMultipleSegmentsRequest req;
    for (size_t i = 0; i < count; i++)
    {
        auto allocSegReq = req.add_allocsegmentrequests();
        allocSegReq->set_name(bufferName);
        allocSegReq->set_segments_count(segmentsCount);
        allocSegReq->set_segments_size(fullSegmentSize);
        allocSegReq->set_buffer_type(buffertype);
        allocSegReq->set_cache_align(cacheAlign);
    }
    Any sendAny;
    sendAny.PackFrom(req);
    Any rcvAny;
    rdma::ProtoClient::exchangeProtoMsg(connection, &sendAny, &rcvAny);

    if (rcvAny.Is<DFIAllocMultipleSegmentsResponse>())
    {
        DFIAllocMultipleSegmentsResponse resResp;
        rcvAny.UnpackTo(&resResp);
        for (int i = 0; i < resResp.responses_size(); i++)
        {
            DFIAllocSegmentsResponse allocResp = resResp.responses(i);
            if (allocResp.return_() == MessageErrors::NO_ERROR)
            {
                offsets.push_back(allocResp.offset());
            }
            else
            {
                Logging::warn("RDMAClient: Got error code " + to_string(allocResp.return_()));
                return false;
            }
        }
        return true;
    }
    else if (rcvAny.Is<DFIAllocSegmentsResponse>())
    {
        Logging::debug(__FILE__, __LINE__, "Received DFIAllocSegmentsResponse");
        DFIAllocSegmentsResponse resResp;
        rcvAny.UnpackTo(&resResp);
        if (resResp.return_() == MessageErrors::NO_ERROR)
        {
            offsets.push_back(resResp.offset());
            return true;
        }
    }
    return false;
}

bool NodeClient::remoteFreeSegments(const string &connection, const NodeID, const string &bufferName)
{
    // DFIFreeSegmentsRequest
    if (!rdma::ProtoClient::isConnected(connection))
    {
        NodeID rdmaNodeID;
        if (!RDMAClient::connect(connection, rdmaNodeID))
        {
            Logging::error(__FILE__, __LINE__, "NodeClient tried to remoteFreeSegments to connection: " + connection + ", but failed to connect.");
            return false;
        }
    }

    DFIFreeSegmentsRequest freeSegmentsRequest;
    freeSegmentsRequest.set_name(bufferName);
    Any anyReq;
    Any anyResp;
    anyReq.PackFrom(freeSegmentsRequest);

    rdma::ProtoClient::exchangeProtoMsg(connection, &anyReq, &anyResp);

    return true;
}

uint64_t NodeClient::getStartRdmaAddrForNode(NodeID nodeid)
{

    auto remote_conn = RDMAClient::getRemoteConnData(DFINodeIDToRDMANodeID[nodeid]);
    return remote_conn.buffer;
}
