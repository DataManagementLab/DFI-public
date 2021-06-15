/**
 * @file NodeClient.h
 * @author cbinnig, lthostrup, tziegler
 * @date 2018-07-06
 */

#pragma once

#include "../../utils/Config.h"
#include "../../utils/Logging.h"
#include "../../rdma-manager/src/rdma/RDMAClient.h"
#include "FlowOptimization.h"
#include "../message/MessageTypes.h"
#include "../message/MessageErrors.h"

namespace dfi
{

class NodeClient : public rdma::RDMAClient<rdma::ReliableRDMA>
{
  typedef rdma::RDMAClient<rdma::ReliableRDMA> RDMAClient;
  public:
    NodeClient(/* args */);
    NodeClient(size_t memsize);
    ~NodeClient();

    bool remoteAllocSegments(const string& connection, const NodeID nodeID, const string& bufferName, const size_t segmentsCount, 
                             const size_t fullSegmentSize, FlowOptimization buffertype, bool cacheAlign, std::vector<size_t>& offsets, size_t count);
    bool remoteFreeSegments(const string& connection, const NodeID nodeID, const string& bufferName);

    uint64_t getStartRdmaAddrForNode(NodeID nodeid);

    bool connect(const string& ipPort, NodeID DFINodeID) {
        NodeID retServerNodeID = 0;
        bool ret = RDMAClient::connect(ipPort, retServerNodeID);
        if (DFINodeIDToRDMANodeID.size() < DFINodeID + 1) {
            DFINodeIDToRDMANodeID.resize(DFINodeID + 1);
        }
        DFINodeIDToRDMANodeID[DFINodeID] = retServerNodeID;
        return ret;
    }
    void write(const NodeID nodeID, size_t offset, const void* memAddr,
             size_t size, bool signaled)
    {
        RDMAClient::write(DFINodeIDToRDMANodeID[nodeID], offset, memAddr, size, signaled);
        
    }
    void read(const NodeID nodeID, size_t offset, const void* memAddr,
            size_t size, bool signaled)
    {
        RDMAClient::read(DFINodeIDToRDMANodeID[nodeID], offset, memAddr, size, signaled);

    }
    void send(const NodeID nodeID, const void* memAddr, size_t size,
            bool signaled)
    {
        RDMAClient::send(DFINodeIDToRDMANodeID[nodeID], memAddr, size, signaled);
    }
    void receive(const NodeID nodeID, const void* memAddr,
               size_t size)
    {
        RDMAClient::receive(DFINodeIDToRDMANodeID[nodeID], memAddr, size);
    }
private:
    std::vector<NodeID> DFINodeIDToRDMANodeID;

    using RDMAClient::send; //Make private
    using RDMAClient::receive; //Make private
    using RDMAClient::write; //Make private
    using RDMAClient::read; //Make private
    using RDMAClient::remoteAlloc;
    using RDMAClient::remoteFree;
    using RDMAClient::setRemoteConnData;


};
} // namespace dfi
