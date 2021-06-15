#pragma once

#include "../dfi/memory/NodeServer.h"


class DFI_Node final : public NodeServer
{
public:
  /**
   * @brief Construct a new dfi node object. A DFI_Node is an instance deployed on nodes for (potentially multiple) targets to identify to.
            * DFI_Node must be addresseable through DFI_Nodes vector in dfi::Config
   * 
   * @param memsize Size of the internal memory. Must be larger than the needed memory for the flows
   * @param port Port listening for out-of-band communication 
   * @param numaNode Performs all memory allocations on the specified numa node. Parameter takes presedence over the rdma::Config::RDMA_NUMAREGION.
   */
  DFI_Node(uint16_t port = dfi::Config::DFI_NODE_PORT, uint64_t memsize = rdma::Config::RDMA_MEMSIZE, int numaNode = rdma::Config::RDMA_NUMAREGION) : NodeServer(memsize, port, numaNode)
  {}
};