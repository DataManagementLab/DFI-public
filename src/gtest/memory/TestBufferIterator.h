/**
 * @file IntegrationTestsAppend.h
 * @author cbinnig, lthostrup, tziegler
 * @date 2018-08-17
 */

#pragma once
#include <gtest/gtest.h>

#include "../../utils/Config.h"

#include "../../dfi/registry/RegistryClient.h"
#include "../../dfi/registry/RegistryServer.h"
#include "../../dfi/memory/BufferHandle.h"
#include "../../dfi/memory/NodeServer.h"
#include "../../dfi/memory/NodeClient.h"
#include "../../dfi/memory/BufferWriter.h"
#include "../../dfi/memory/BufferWriter.h"

#include <atomic>


class TestBufferIterator : public testing::Test 
{

public:
  void SetUp() override;
  void TearDown() override;


  static std::atomic<int> bar;    // Counter of threads, faced barrier.
  static std::atomic<int> passed; // Number of barriers, passed by all threads.

protected:
  RegistryClient *m_regClient;
  RegistryServer *m_regServer;
  NodeServer *m_nodeServer; 
  NodeClient *m_nodeClient; 



template <class DataType> 
class BufferWriterClient : public rdma::Thread
{
  BufferHandle* buffHandle = nullptr;
  string& bufferName = "";
  std::vector<DataType> *dataToWrite = nullptr; //tuple<ptr to data, size in bytes>

public: 

  BufferWriterClient(string& bufferName, std::vector<DataType> *dataToWrite, int numThread = 4) : 
    Thread(), bufferName(bufferName), dataToWrite(dataToWrite), NUMBER_THREADS(numThread) {}

  int NUMBER_THREADS;
  
  void run() 
  {
    //ARRANGE
    RegistryClient registry_client;
    BufferWriter buffWriter(bufferName, registry_client, Config::DFI_SOURCE_SEGMENT_COUNT);

    barrier_wait(); //Use barrier to simulate concurrent appends between BufferWriters

    //ACT
    for(size_t i = 0; i < dataToWrite->size(); i++)
    {
      buffWriter.add(&dataToWrite->operator[](i), sizeof(DataType));
    }

    buffWriter.close();
  }

    void barrier_wait()
    {
        // std::cout << "Enter Barrier" << '\n';
        int passed_old = passed.load(std::memory_order_relaxed);

        if (bar.fetch_add(1) == (NUMBER_THREADS - 1))
        {
            // The last thread, faced barrier.
            bar = 0;
            // Synchronize and store in one operation.
            passed.store(passed_old + 1, std::memory_order_release);
        }
        else
        {
            // Not the last thread. Wait others.
            while (passed.load(std::memory_order_relaxed) == passed_old)
            {
            };
            // Need to synchronize cache with other threads, passed barrier.
            std::atomic_thread_fence(std::memory_order_acquire);
        }
    }
};

};