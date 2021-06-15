/**
 * @file TestBufferWriterLat.h
 * @author cbinnig, lthostrup, tziegler
 * @date 2018-08-17
 */

#pragma once
#include <gtest/gtest.h>

#include "../../utils/Config.h"

#include "../../dfi/memory/NodeServer.h" 
#include "../../dfi/memory/NodeClient.h"
#include "../../dfi/registry/RegistryClient.h"
#include "../../dfi/registry/RegistryServer.h"
#include "../../dfi/memory/BufferWriter.h"
#include "../../dfi/memory/BufferWriter.h"

#include <atomic>

class TestBufferWriterLat : public testing::Test  {
 
 public:
  void SetUp() override;
  void TearDown() override;
 

  static std::atomic<int> bar;    // Counter of threads, faced barrier.
  static std::atomic<int> passed; // Number of barriers, passed by all threads.
  static const int NUMBER_THREADS = 2;

  DFI_SEGMENT_FOOTER_t *readSegmentFooter(size_t offset);
  DFI_SEGMENT_FOOTER_t *readSegmentFooter(BufferSegment* segment);

protected:
  NodeServer* m_nodeServer;
  RegistryClient* m_regClient;
  RegistryServer *m_regServer;

 
struct TestData
{
  int a;
  int b;
  int c;
  int d;
  TestData(int a, int b, int c, int d) : a(a), b(b), c(c), d(d){}
};

template <class DataType>
class BufferWriterClient : public rdma::Thread
{
  NodeServer* nodeServer = nullptr;
  RegistryClient* regClient = nullptr;
  BufferHandle* buffHandle = nullptr;
  string& buffername = "";
  std::vector<DataType> *dataToWrite = nullptr; //tuple<ptr to data, size in bytes>

public: 

  BufferWriterClient(NodeServer* nodeServer, string& bufferName, std::vector<DataType> *dataToWrite) : 
    Thread(), nodeServer(nodeServer), buffername(bufferName), dataToWrite(dataToWrite) {}

  void run() 
  {
    //ARRANGE

    RegistryClient registry_client;
    BufferWriter buffWriter(buffername, registry_client, Config::DFI_SOURCE_SEGMENT_COUNT);

    barrier_wait();

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
