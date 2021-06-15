#include "flow_barrier_tests.h"

void TestFlowBarrier::SetUp()
{
  //Setup Test DFI
  rdma::Config::RDMA_MEMSIZE = 1024ul * 1024 * 1;  //8MB
  Config::DFI_SOURCE_SEGMENT_COUNT = 8;
  Config::DFI_REGISTRY_SERVER = rdma::Config::getIP(rdma::Config::RDMA_INTERFACE);
  Config::DFI_REGISTRY_PORT = 5300;
  Config::DFI_FULL_SEGMENT_SIZE = (512 + sizeof(DFI_SEGMENT_FOOTER_t));
  Config::DFI_SEGMENTS_PER_RING = 5;
  // Config::LOGGING_LEVEL = 1;
  // rdma::Config::LOGGING_LEVEL = 1;
  rdma::Config::SEQUENCER_IP = rdma::Config::getIP(rdma::Config::RDMA_INTERFACE);
  Config::DFI_NODES.clear();    
  Config::DFI_NODES.push_back(rdma::Config::getIP(rdma::Config::RDMA_INTERFACE) + ":" + to_string(Config::DFI_NODE_PORT));

  m_regServer = std::make_unique<RegistryServer>();

  m_nodeServer = std::make_unique<NodeServer>(rdma::Config::RDMA_MEMSIZE, Config::DFI_NODE_PORT);

  DFI_Schema schema({{"value", dfi::TypeId::BIGINT}});

  std::vector<target_t> targets = {{1, 1}, {2, 1}};
  if (DFI_Shuffle_flow_init(flow_name, 3, targets, schema, 0) != DFI_SUCCESS)
  {
    FAIL() << "TestFlowBarrier::SetUp could not initialize shuffle flow!";
  }
}

void TestFlowBarrier::TearDown()
{

}


TEST_F(TestFlowBarrier, testSyncThreeSources)
{
  DFI_Flow_barrier_init(flow_name);
  
  bool firstSourceArrived = false;
  bool secondSourceArrived = false;
  bool thirdSourceArrived = false;

  std::thread source_thread1([&]() {
    DFI_Flow_barrier barrier(flow_name);
    firstSourceArrived = true;
    barrier.arriveWaitSources();

    ASSERT_TRUE(secondSourceArrived);
    ASSERT_TRUE(thirdSourceArrived);
  });

  std::thread source_thread2([&]() {
    DFI_Flow_barrier barrier(flow_name);
    thirdSourceArrived = true;
    barrier.arriveWaitSources();

    ASSERT_TRUE(firstSourceArrived);
    ASSERT_TRUE(thirdSourceArrived);
  });

  usleep(100000);
  DFI_Flow_barrier barrier(flow_name);
  secondSourceArrived = true;
  
  barrier.arriveWaitSources();
  ASSERT_TRUE(firstSourceArrived);
  ASSERT_TRUE(secondSourceArrived);

  source_thread1.join();
  source_thread2.join();
  std::cout << "joining" << std::endl;
}

TEST_F(TestFlowBarrier, testSyncTwoTargets)
{
  DFI_Flow_barrier_init(flow_name);
  
  bool firstTargetArrived = false;
  bool secondTargetArrived = false;

  std::thread target_thread1([&]() {
    DFI_Flow_barrier barrier(flow_name);
    firstTargetArrived = true;
    barrier.arriveWaitTargets();

    ASSERT_TRUE(secondTargetArrived);
  });

  usleep(100000);
  DFI_Flow_barrier barrier(flow_name);
  secondTargetArrived = true;
  
  barrier.arriveWaitTargets();
  ASSERT_TRUE(firstTargetArrived);

  target_thread1.join();
}