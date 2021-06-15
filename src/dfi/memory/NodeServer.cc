#include "NodeServer.h"

NodeServer::NodeServer(): NodeServer(rdma::Config::RDMA_MEMSIZE, Config::DFI_NODE_PORT) {}

NodeServer::NodeServer(uint64_t memsize): NodeServer(memsize, Config::DFI_NODE_PORT) {}

NodeServer::NodeServer(uint64_t memsize, uint16_t port): NodeServer(memsize, port, (int)rdma::Config::RDMA_NUMAREGION) {}

NodeServer::NodeServer(uint64_t memsize, uint16_t port, int numaNode) : rdma::RDMAServer<rdma::ReliableRDMA>("NodeServer", port, memsize, numaNode) {
    if (!rdma::RDMAServer<rdma::ReliableRDMA>::isRunning())
    {
      rdma::RDMAServer<rdma::ReliableRDMA>::startServer();
    }
}