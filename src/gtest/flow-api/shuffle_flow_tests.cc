#include "shuffle_flow_tests.h"
#include <numeric>
void TestShuffleFlow::SetUp()
{
  //Setup Test DFI
  rdma::Config::RDMA_MEMSIZE = 1024ul * 1024 * 1;
  Config::DFI_SOURCE_SEGMENT_COUNT = 8;
  Config::DFI_REGISTRY_SERVER = rdma::Config::getIP(rdma::Config::RDMA_INTERFACE);
  Config::DFI_REGISTRY_PORT = 5300;
  Config::DFI_FULL_SEGMENT_SIZE = (512 + sizeof(DFI_SEGMENT_FOOTER_t));
  Config::DFI_SEGMENTS_PER_RING = 5;
  rdma::Config::SEQUENCER_IP = rdma::Config::getIP(rdma::Config::RDMA_INTERFACE);
  Config::LOGGING_LEVEL = 1;
  rdma::Config::LOGGING_LEVEL = 1;
  uint16_t server1Port = 5400;
  uint16_t server2Port = 5401;
  Config::DFI_NODES = {rdma::Config::getIP(rdma::Config::RDMA_INTERFACE) + ":" + to_string(server1Port), 
                       rdma::Config::getIP(rdma::Config::RDMA_INTERFACE) + ":" + to_string(server2Port)};

  m_regServer = std::make_unique<RegistryServer>();

  m_nodeServer1 = std::make_unique<DFI_Node>(server1Port);
  m_nodeServer2 = std::make_unique<DFI_Node>(server2Port);

}

void TestShuffleFlow::TearDown()
{

}

TEST_F(TestShuffleFlow, testAsyncConsume)
{
  DFI_Schema schema({{"value", dfi::TypeId::BIGINT}});
  std::vector<target_t> targets = {{1, 1}, {2, 1}};
  if (DFI_Shuffle_flow_init(flow_name, 1, targets, schema, 0) != DFI_SUCCESS)
  {
    FAIL() << "TestFlowBarrier::SetUp could not initialize shuffle flow!";
  }

  DFI_Shuffle_flow_source source(flow_name);

  DFI_Shuffle_flow_target target(flow_name, 1);

  dfi::Tuple tuple;
  size_t tuples_consumed = -1;
  ASSERT_EQ(target.consumeAsync(tuple, tuples_consumed), DFI_NO_TUPLE);
  ASSERT_EQ(tuples_consumed, 0);

  size_t data = 42;
  source.push(&data, (TargetID)1);
  source.flush();

  usleep(10); //Ensure NIC has written data to memory

  ASSERT_EQ(target.consumeAsync(tuple, tuples_consumed), DFI_SUCCESS);
  ASSERT_EQ(tuples_consumed, 1);

  ASSERT_EQ(target.consumeAsync(tuple), DFI_NO_TUPLE);
}

TEST_F(TestShuffleFlow, testLatencyFlow)
{
  Config::DFI_SEGMENTS_PER_RING = 10;
  DFI_Schema schema({{"value", dfi::TypeId::BIGINT}});

  std::vector<target_t> targets = {{1, 1}, {2, 1}};
  if (DFI_Shuffle_flow_init(flow_name, 1, targets, schema, 0, DFI_Shuffle_flow_optimization::Latency) != DFI_SUCCESS)
  {
    FAIL() << "TestFlowBarrier::SetUp could not initialize shuffle flow!";
  }

  DFI_Shuffle_flow_source source(flow_name);

  std::thread target_thread1([&]() {
    DFI_Shuffle_flow_target target(flow_name, 1);
    dfi::Tuple tuple = target.create_tuple();
    while (target.consume(tuple) != DFI_FLOW_FINISHED)
    {
      ASSERT_EQ(tuple.getAs<uint64_t>("value"), 1);
      usleep(100);
    }
  });

  std::thread target_thread2([&]() {
    DFI_Shuffle_flow_target target(flow_name, 2);
    dfi::Tuple tuple = target.create_tuple();
    while (target.consume(tuple) != DFI_FLOW_FINISHED)
    {
      ASSERT_EQ(tuple.getAs<uint64_t>("value"), 2);
      usleep(100);
    }
  });

  for (size_t i = 0; i < 1000; i++)
  {
    dfi::Tuple tuple;
    size_t data = (i % 2) + 1;
    source.push(&data, (TargetID)data);  
  }
  source.close();

  target_thread1.join();
  target_thread2.join();
}


TEST_F(TestShuffleFlow, testBandwidthFlow)
{
  Config::DFI_FULL_SEGMENT_SIZE = (sizeof(uint64_t) + sizeof(DFI_SEGMENT_FOOTER_t));
  Config::DFI_SEGMENTS_PER_RING = 10;
  DFI_Schema schema({{"value", dfi::TypeId::BIGINT}});

  std::vector<target_t> targets = {{1, 1}, {2, 1}};
  if (DFI_Shuffle_flow_init(flow_name, 1, targets, schema, 0, DFI_Shuffle_flow_optimization::Bandwidth) != DFI_SUCCESS)
  {
    FAIL() << "TestFlowBarrier::SetUp could not initialize shuffle flow!";
  }

  DFI_Shuffle_flow_source source(flow_name);

  std::thread target_thread1([&]() {
    DFI_Shuffle_flow_target target(flow_name, 1);
    dfi::Tuple tuple = target.create_tuple();
    while (target.consume(tuple) != DFI_FLOW_FINISHED)
    {
      ASSERT_EQ(tuple.getAs<uint64_t>("value"), 1);
    }
  });

  std::thread target_thread2([&]() {
    DFI_Shuffle_flow_target target(flow_name, 2);
    dfi::Tuple tuple = target.create_tuple();
    while (target.consume(tuple) != DFI_FLOW_FINISHED)
    {
      ASSERT_EQ(tuple.getAs<uint64_t>("value"), 2);
      usleep(100);
    }
  });

  for (size_t i = 0; i < 1000; i++)
  {
    dfi::Tuple tuple;
    size_t data = (i % 2) + 1;
    source.push(&data, (TargetID)data);
  }
  source.close();

  target_thread1.join();
  target_thread2.join();
}

TEST_F(TestShuffleFlow, testAsyncConsumeOnClose)
{
  DFI_Schema schema({{"value", dfi::TypeId::BIGINT}});
  std::vector<target_t> targets = {{1, 1}, {2, 1}};
  if (DFI_Shuffle_flow_init(flow_name, 1, targets, schema, 0) != DFI_SUCCESS)
  {
    FAIL() << "TestFlowBarrier::SetUp could not initialize shuffle flow!";
  }
  DFI_Shuffle_flow_source source(flow_name);

  DFI_Shuffle_flow_target target(flow_name, 1);

  dfi::Tuple tuple;
  size_t tuples_consumed = -1;
  ASSERT_EQ(target.consumeAsync(tuple, tuples_consumed), DFI_NO_TUPLE);
  ASSERT_EQ(tuples_consumed, 0);

  size_t data = 42;
  source.push(&data, (TargetID)1);
  source.close();

  usleep(10); //Ensure NIC has written data to memory

  ASSERT_EQ(target.consumeAsync(tuple, tuples_consumed), DFI_SUCCESS);
  ASSERT_EQ(tuples_consumed, 1);

  ASSERT_EQ(target.consume(tuple), DFI_FLOW_FINISHED);
}


TEST_F(TestShuffleFlow, testPushBulk)
{
  DFI_Schema schema({{"value", dfi::TypeId::BIGINT}});
  std::vector<target_t> targets = {{1, 1}, {2, 1}};
  if (DFI_Shuffle_flow_init(flow_name, 1, targets, schema, 0) != DFI_SUCCESS)
  {
    FAIL() << "TestFlowBarrier::SetUp could not initialize shuffle flow!";
  }
  DFI_Shuffle_flow_source source(flow_name);

  DFI_Shuffle_flow_target target(flow_name, 1);

  uint64_t raw_tuples[128];
  std::iota(raw_tuples, raw_tuples+128, 0);

  source.pushBulk(raw_tuples, (TargetID)1, 32);
  source.pushBulk(raw_tuples+32, (TargetID)1, 32);
  source.pushBulk(raw_tuples+64, (TargetID)1, 32);
  source.pushBulk(raw_tuples+96, (TargetID)1, 32);
  source.close();

  dfi::Tuple tuple;
  size_t tuple_count = 0;
  while(target.consume(tuple) != DFI_FLOW_FINISHED)
  {
    ASSERT_EQ(tuple.getAs<uint64_t>(0), tuple_count);
    tuple_count++;
  }
  ASSERT_EQ(tuple_count, 128);
}


TEST_F(TestShuffleFlow, testFlowCleanup)
{
  Config::DFI_CLEANUP_FLOWS = true;
  Config::DFI_SEGMENTS_PER_RING = 1024; //Ensure only one flow can be created in the allocated size in the dfi node
  DFI_Schema schema({{"value", dfi::TypeId::BIGINT}});
  std::vector<target_t> targets = {{1, 1}};
  
  {
    if (DFI_Shuffle_flow_init(flow_name, 1, targets, schema, 0) != DFI_SUCCESS)
    {
      FAIL() << "TestFlowBarrier::SetUp could not initialize shuffle flow!";
    }
    DFI_Shuffle_flow_source source(flow_name);

    DFI_Shuffle_flow_target target(flow_name, 1);

    uint64_t raw_tuples[20];
    std::iota(raw_tuples, raw_tuples+20, 0);

    source.pushBulk(raw_tuples, (TargetID)1, 20);
    source.close();

    dfi::Tuple tuple;
    size_t tuple_count = 0;
    while(target.consume(tuple) != DFI_FLOW_FINISHED)
    {
      ASSERT_EQ(tuple.getAs<uint64_t>(0), tuple_count);
      tuple_count++;
    }
    ASSERT_EQ(tuple_count, 20);

    ASSERT_EQ(DFI_Shuffle_flow_init(flow_name, 1, targets, schema, 0), DFI_FAILURE); //It should not be possible to initialize flow within scope since old is not cleaned up
  } //Destroy target object

  //Assert new flow with same name can be initialized after cleanup --> Registry is cleaned up, and enough memory is available only if old buffers are freed
  ASSERT_EQ(DFI_Shuffle_flow_init(flow_name, 1, targets, schema, 0), DFI_SUCCESS);

}

TEST_F(TestShuffleFlow, testEmptyFlush)
{
  //Flow initialization
  size_t source_count = 1;
  std::vector<target_t> targets{{1,1}, {2,2}}; // target id 1 --> node1 && target id 2 --> node 2
  DFI_Schema schema({{"key", TypeId::INT},{"value", TypeId::INT}});
  DFI_Shuffle_flow_init(flow_name, source_count, targets, schema, 0);

  //Flow execution
  DFI_Shuffle_flow_source source(flow_name);
  DFI_Shuffle_flow_target target1(flow_name, 1);
  DFI_Shuffle_flow_target target2(flow_name, 2);

  std::pair<int, int> data;
  data = {3,10};
  source.push(&data, (TargetID)2);
  source.flush(); //Flush without sending anything to target 1

  usleep(10000); //sleep 10ms

  dfi::Tuple tuple1;
  ASSERT_EQ(target1.consumeAsync(tuple1), DFI_NO_TUPLE);

  dfi::Tuple tuple2;
  ASSERT_EQ(target2.consume(tuple2), DFI_SUCCESS);

  ASSERT_EQ(tuple2.getAs<int>("key"), 3);
}

TEST_F(TestShuffleFlow, two_src_two_targets_many_tuples)
{
  string flow_name = "Simple Shuffle Flow";
  std::cout << "TestShuffleFlow::two_src_two_targets_many_tuples" << std::endl;
  // ------- MASTER -------
  DFI_Schema schema({{"key", dfi::TypeId::BIGINT}, {"value", dfi::TypeId::BIGINT}});
  size_t shuffle_key_index = 0;
  size_t sources_count = 2;
  vector<target_t> targets = {{1, 1}, {2, 2}};
  DFI_Shuffle_flow_init(flow_name, sources_count, targets, schema, shuffle_key_index);

  // ------- Flow Source 1 -------
  std::thread source_thread1([flow_name]() {
    std::cout << "Creating Flow Source 1" << '\n';
    DFI_Shuffle_flow_source source(flow_name, 8);
    auto f_distr=[](const Tuple &t){(void)t; return (TargetID)1;};

    for(uint64_t i = 0; i < 1000000; i++)
    {
      uint64_t raw_tuple[] = {i, i};
      source.push(Tuple(raw_tuple), f_distr);
    }
  });

  // ------- Flow Source 2 -------
  std::thread source_thread2([flow_name]() {
    std::cout << "Creating Flow Source 2" << '\n';
    DFI_Shuffle_flow_source source(flow_name, 8);
    for(uint64_t i = 1000000; i < 2000000; i++)
    {
      uint64_t raw_tuple[] = {i, i};
      source.push(raw_tuple, (TargetID)2);
    }
  });

  // ------- Flow Target 1 -------
  vector<uint64_t> consumed_keys_1;
  std::thread target_thread1([flow_name, &consumed_keys_1]() {
    std::cout << "Creating Flow Target 1" << '\n';
    DFI_Shuffle_flow_target target(flow_name, 1);
    dfi::Tuple tuple;
    while(target.consume(tuple) != DFI_FLOW_FINISHED)
    {
      uint64_t key = *(uint64_t*)tuple.getDataPtr("key");
      // std::cout << "1Consumed " << key << std::endl;
      consumed_keys_1.push_back(key);
    }
  });
  // ------- Flow Target 2 -------
  vector<uint64_t> consumed_keys_2;
  std::thread target_thread2([flow_name, &consumed_keys_2]() {
    std::cout << "Creating Flow Target 2" << '\n';
    DFI_Shuffle_flow_target target(flow_name, 2);
    dfi::Tuple tuple;
    while(target.consume(tuple) != DFI_FLOW_FINISHED)
    {
      uint64_t key = *(uint64_t*)tuple.getDataPtr("key");
      // std::cout << "2Consumed " << key << std::endl;
      consumed_keys_2.push_back(key);
    }
  });

  source_thread1.join();
  source_thread2.join();
  target_thread1.join();
  target_thread2.join();
  
  ASSERT_EQ((size_t)1000000, consumed_keys_1.size()) << "Size of consumed tuples for Flow Target 1 does not match expected.";
  ASSERT_EQ((size_t)1000000, consumed_keys_2.size()) << "Size of consumed tuples for Flow Target 2 does not match expected.";
}


TEST_F(TestShuffleFlow, two_src_two_targets_simple_test)
{
  string flow_name = "Simple Shuffle Flow";
  std::cout << "TestShuffleFlow::two_src_two_targets_simple_test" << std::endl;
  // ------- MASTER -------
  DFI_Schema schema({{"key", dfi::TypeId::BIGINT}, {"value", dfi::TypeId::BIGINT}});
  size_t shuffle_key_index = 0;
  size_t sources_count = 2;
  vector<target_t> targets = {{1, 1}, {2, 2}};
  DFI_Shuffle_flow_init(flow_name, sources_count, targets, schema, shuffle_key_index);

  // ------- Flow Source 1 -------
  std::thread source_thread1([flow_name]() {
    std::cout << "Creating Flow Source 1" << '\n';
    DFI_Shuffle_flow_source source(flow_name, 8);
    auto f_distr=[](const Tuple &t){(void)t; return (TargetID)1;};

    for(uint64_t i = 0; i < 50; i++)
    {
      uint64_t raw_tuple[] = {i, i};
      source.push(Tuple(raw_tuple), f_distr);
    }
  });

  // ------- Flow Source 2 -------
  std::thread source_thread2([flow_name]() {
    std::cout << "Creating Flow Source 2" << '\n';
    DFI_Shuffle_flow_source source(flow_name, 8);
    for(uint64_t i = 50; i < 100; i++)
    {
      uint64_t raw_tuple[] = {i, i};
      source.push(raw_tuple, (TargetID)2);
    }
  });

  // ------- Flow Target 1 -------
  vector<uint64_t> consumed_keys_1;
  std::thread target_thread1([flow_name, &consumed_keys_1]() {
    std::cout << "Creating Flow Target 1" << '\n';
    DFI_Shuffle_flow_target target(flow_name, 1);
    dfi::Tuple tuple;
    while(target.consume(tuple) != DFI_FLOW_FINISHED)
    {
      uint64_t key = *(uint64_t*)tuple.getDataPtr("key");
      // std::cout << "1Consumed " << key << std::endl;
      consumed_keys_1.push_back(key);
    }
  });
  // ------- Flow Target 2 -------
  vector<uint64_t> consumed_keys_2;
  std::thread target_thread2([flow_name, &consumed_keys_2]() {
    std::cout << "Creating Flow Target 2" << '\n';
    DFI_Shuffle_flow_target target(flow_name, 2);
    dfi::Tuple tuple;
    while(target.consume(tuple) != DFI_FLOW_FINISHED)
    {
      uint64_t key = *(uint64_t*)tuple.getDataPtr("key");
      // std::cout << "2Consumed " << key << std::endl;
      consumed_keys_2.push_back(key);
    }
  });

  source_thread1.join();
  source_thread2.join();
  target_thread1.join();
  target_thread2.join();
  
  ASSERT_EQ((size_t)50, consumed_keys_1.size()) << "Size of consumed tuples for Flow Target 1 does not match expected.";
  ASSERT_EQ((size_t)50, consumed_keys_2.size()) << "Size of consumed tuples for Flow Target 2 does not match expected.";
}

TEST_F(TestShuffleFlow, testShuffleFlowInit)
{
  DFI_Schema schema({{"Attr1", TypeId::BIGINT}});

  //Targets parameter contain duplicate targetId
  ASSERT_EQ(
    (int)DFI_FAILURE,
    DFI_Shuffle_flow_init("flow name", 1, {{1,1}, {1,1}}, schema, 0, DFI_Shuffle_flow_optimization::Latency)
  );

  //Targets parameter contains nodeId not found in DFI_NODES vector
  ASSERT_EQ(
    (int)DFI_FAILURE,
    DFI_Shuffle_flow_init("flow name", 1, {{2,5}, {1,1}}, schema, 1, DFI_Shuffle_flow_optimization::Bandwidth)
  );

  //No targets provided
  ASSERT_EQ(
    (int)DFI_FAILURE,
    DFI_Shuffle_flow_init("flow name", 1, {}, schema, 1, DFI_Shuffle_flow_optimization::Bandwidth)
  );

  //Shuffle key refers to attribute not in schema
  ASSERT_EQ(
    (int)DFI_FAILURE,
    DFI_Shuffle_flow_init("flow name", 1, {{1,1}}, schema, 10, DFI_Shuffle_flow_optimization::Bandwidth)
  );

}

