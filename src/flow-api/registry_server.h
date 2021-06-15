#pragma once

#include "../dfi/registry/RegistryServer.h"


class DFI_Registry_server final: public RegistryServer
{
public:
    /**
     * @brief Construct a new dfi registry server object. Registry Server is a central DFI instance that coordinates flow and stores flow meta data.
     * Important: DFI_Registry_server must be constructed before any flows are initialized.
     * 
     * @param mem_size 
     */
    DFI_Registry_server(size_t mem_size = Config::DFI_REGISTRY_RDMA_MEM) : RegistryServer(mem_size) {}
};