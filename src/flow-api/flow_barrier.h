/**
 * @file flow_barrier_init.h
 * @author lthostrup
 * @date 2020-03-01
 */

#pragma once

#include "../utils/Config.h"
#include "schema.h"
#include "../dfi/registry/RegistryClient.h"
#include "../dfi/type/ErrorCodes.h"


class DFI_Flow_barrier
{   
    using rdmaclient_t = rdma::RDMAClient<rdma::ReliableRDMA>; //Reliable client
public:

    /**
     * @brief Construct a new dfi flow barrier object. Both Flow and flow barrier must be pre-initialized.
     * 
     * @param name Unique flow name identifier
     */
    DFI_Flow_barrier(std::string &name)
    {
        dfi::RegistryClient regClient;

        while (!regClient.getFlowBarrierOffset(name, m_barrierOffset))
        {
            usleep(Config::DFI_SLEEP_INTERVAL);
        }

        m_flowHandle = regClient.retrieveFlowHandle(name);
        if(m_flowHandle == nullptr)
        {
            Logging::error(__FILE__, __LINE__, "DFI_Flow_barrier could not get flow handle for barrier. Flow name: " + name);
            return;
        }

        auto rdmaclient_size = rdma::Config::CACHELINE_SIZE;
        m_rdmaclient = std::make_unique<rdmaclient_t>(rdmaclient_size);

        m_rdmaclient->connect(Config::DFI_REGISTRY_SERVER + ":" + to_string(Config::DFI_REGISTRY_PORT), m_registryServerID);
        m_localCounter = reinterpret_cast<size_t*>(m_rdmaclient->localAlloc(sizeof(size_t)));
    };

    ~DFI_Flow_barrier() = default;

    /**
     * @brief Arrive at barrier and wait for all sources to arrive. Should exclusively be used to sync sources (i.e. not concurrently sync targets)
     * 
     */
    void arriveWaitSources()
    {
        arriveWait(m_flowHandle->sources.size());
    };

    /**
     * @brief Arrive at barrier and wait for all targets to arrive. Should exclusively be used to sync targets (i.e. not concurrently sync source)
     * 
     */
    void arriveWaitTargets()
    {
        arriveWait(m_flowHandle->targets.size());
    };

    /**
     * @brief Arrive at barrier and wait for all sources and targets to arrive.
     * 
     */
    void arriveWaitAll()
    {
        arriveWait(m_flowHandle->targets.size() + m_flowHandle->sources.size());
    };

    /**
     * @brief Resets the barrier for later reuse
     * 
     */
    void reset()
    {
        *m_localCounter = 0;
        m_rdmaclient->write(m_registryServerID, m_barrierOffset, m_localCounter, sizeof(size_t), true);
    };

private:
    size_t m_barrierOffset;
    size_t *m_localCounter;
    std::unique_ptr<dfi::FlowHandle> m_flowHandle;
    std::unique_ptr<rdmaclient_t> m_rdmaclient;
    NodeID m_registryServerID;

    void arriveWait(size_t count)
    {
        //fetch and add
        m_rdmaclient->fetchAndAdd(m_registryServerID, m_barrierOffset, m_localCounter, sizeof(size_t), true);
        ++(*m_localCounter);
        //rdma read until count
        while (*m_localCounter != count)
        {
            m_rdmaclient->read(m_registryServerID, m_barrierOffset, m_localCounter, sizeof(size_t), true);
        }
    };
};