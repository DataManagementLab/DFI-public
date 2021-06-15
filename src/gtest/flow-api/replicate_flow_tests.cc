#include "replicate_flow_tests.h"

void TestReplicateFlow::SetUp()
{
    //Setup Test DFI
    rdma::Config::RDMA_MEMSIZE = 1024ul * 1024 * 8;  //8MB
    Config::DFI_FULL_SEGMENT_SIZE = (512 + sizeof(DFI_SEGMENT_FOOTER_t));
    Config::DFI_SOURCE_SEGMENT_COUNT = 8;
    Config::DFI_REGISTRY_SERVER = rdma::Config::getIP(rdma::Config::RDMA_INTERFACE);
    Config::DFI_REGISTRY_PORT = 5300;
    // Config::LOGGING_LEVEL = 1;
    // rdma::Config::LOGGING_LEVEL = 1;
    rdma::Config::SEQUENCER_IP = rdma::Config::getIP(rdma::Config::RDMA_INTERFACE);


    Config::DFI_NODES.clear();    
    Config::DFI_NODES.push_back(rdma::Config::getIP(rdma::Config::RDMA_INTERFACE) + ":" + to_string(Config::DFI_NODE_PORT));
    Config::DFI_NODES.push_back(rdma::Config::getIP(rdma::Config::RDMA_INTERFACE) + ":" + to_string(Config::DFI_NODE_PORT+1));

    m_regServer = std::make_unique<RegistryServer>();

    m_nodeServer1 = std::make_unique<NodeServer>(rdma::Config::RDMA_MEMSIZE, Config::DFI_NODE_PORT);
    m_nodeServer2 = std::make_unique<NodeServer>(rdma::Config::RDMA_MEMSIZE, Config::DFI_NODE_PORT+1);

    Config::DFI_MULTICAST_ADDRS = {
        rdma::Config::getIP(rdma::Config::RDMA_INTERFACE),
    };
}

void TestReplicateFlow::TearDown()
{
}


TEST_F(TestReplicateFlow, simpleReplicate)
{
  string flow_name = "Simple Replicate Flow";
  std::cout << "simpleReplicate" << std::endl;
  // ------- MASTER -------
  DFI_Schema schema({{"value", dfi::TypeId::BIGINT}});
  std::vector<source_t> sources = {{1,0}};
  std::vector<target_t> targets = {{1,1}, {2,1}};
  DFI_Replicate_flow_init(flow_name, sources, targets, schema);

  // ------- Flow Source 1 -------
  std::thread source_thread1([flow_name]() {
    std::cout << "Creating Flow Source 1" << '\n';
    DFI_Replicate_flow_source source(flow_name, 1, 8);
    for(uint64_t i = 0; i < 100; i++)
    {
      uint64_t raw_tuple[] = {i};
      source.push(Tuple(raw_tuple));
    }
  });


  // ------- Flow Target 1 -------
  std::vector<uint64_t> consumed_params_1;
  std::thread target_thread1([flow_name, &consumed_params_1]() {
    std::cout << "Creating Flow Target 1" << '\n';
    DFI_Replicate_flow_target target(flow_name, 1);
    dfi::Tuple tuple = target.create_tuple();
    while(target.consume(tuple) != DFI_FLOW_FINISHED)
    {
      consumed_params_1.push_back(*(uint64_t*)tuple.getDataPtr("value"));
    }
  });
  // ------- Flow Target 2 -------
  std::vector<uint64_t> consumed_params_2;
  std::thread target_thread2([flow_name, &consumed_params_2]() {
    std::cout << "Creating Flow Target 2" << '\n';
    DFI_Replicate_flow_target target(flow_name, 2);    
    dfi::Tuple tuple = target.create_tuple();
    while(target.consume(tuple) != DFI_FLOW_FINISHED)
    {
      consumed_params_2.push_back(*(uint64_t*)tuple.getDataPtr("value"));
    }
  });

  source_thread1.join();
  target_thread1.join();
  target_thread2.join();

  int i = 0;
  for(auto key : consumed_params_1)
    ASSERT_EQ((size_t)(i++), key) << "Size of consumed tuples for Flow Target does not match expected.";

    
  i = 0;
  for(auto key : consumed_params_2)
    ASSERT_EQ((size_t)(i++), key) << "Size of consumed tuples for Flow Target does not match expected.";
}


TEST_F(TestReplicateFlow, onesided_1src_2tgt)
{
    string flow_name = "One-sided Replicate Flow";

    // ------- MASTER -------
    DFI_Schema schema({{"value", dfi::TypeId::BIGINT}});
    std::vector<source_t> sources = {{1, 0}};
    std::vector<target_t> targets = {{1, 1}, {2, 1}};
    DFI_Replicate_flow_init(flow_name, sources, targets, schema);
    size_t tuple_count = 100;
    // ------- Flow Source 1 -------
    std::thread source_thread1([&]() {
        DFI_Replicate_flow_source source(flow_name, 1, 8);
        for (uint64_t i = 0; i < tuple_count; i++)
        {
            uint64_t raw_tuple = i;
            source.push(&raw_tuple);
        }
    });

    // ------- Flow Target 1 -------
    std::vector<uint64_t> consumed_params_1;
    std::thread target_thread1([&]() {
        DFI_Replicate_flow_target target(flow_name, 1);
        dfi::Tuple tuple = target.create_tuple();
        while (target.consume(tuple) != DFI_FLOW_FINISHED)
        {
            consumed_params_1.push_back(tuple.getAs<uint64_t>("value"));
        }
    });
    // ------- Flow Target 2 -------
    std::vector<uint64_t> consumed_params_2;
    std::thread target_thread2([&]() {
        DFI_Replicate_flow_target target(flow_name, 2);
        dfi::Tuple tuple = target.create_tuple();
        while (target.consume(tuple) != DFI_FLOW_FINISHED)
        {
            consumed_params_2.push_back(tuple.getAs<uint64_t>("value"));
        }
    });

    source_thread1.join();
    target_thread1.join();
    target_thread2.join();

    EXPECT_EQ(tuple_count, consumed_params_1.size()) << "Size of consumed tuples for Flow Target 1 does not match expected.";
    ASSERT_EQ(tuple_count, consumed_params_2.size()) << "Size of consumed tuples for Flow Target 2 does not match expected.";
    
    size_t i = 0;
    for (auto key : consumed_params_1) {
        ASSERT_EQ(i++, key) << "Consumed tuple for Flow Target 1 does not match expected.";
    }

    i = 0;
    for (auto key : consumed_params_2) {
        ASSERT_EQ(i++, key) << "Consumed tuple for Flow Target 2 does not match expected.";
    }
}

TEST_F(TestReplicateFlow, multicast_1src_2tgt_nonordered)
{
    Config::DFI_NODES.push_back(rdma::Config::getIP(rdma::Config::RDMA_INTERFACE) + ":" + to_string(Config::DFI_NODE_PORT+2)); //Node for source
    string flow_name = "Multicast Replicate Flow";

    // ------- MASTER -------
    DFI_Schema schema({{"value", dfi::TypeId::BIGINT}});
    std::vector<source_t> sources = {{1, 3}};
    std::vector<target_t> targets = {{1, 0}, {2, 0}};
    DFI_Replicate_flow_init(flow_name, sources, targets, schema, DFI_Replicate_flow_optimization::LatencyMulticast);
    size_t tuple_count = 100;

    // ------- Flow Target 1 -------
    std::vector<uint64_t> consumed_params_1;
    std::thread target_thread1([&]() {
        DFI_Replicate_flow_target target(flow_name, 1);
        dfi::Tuple tuple = target.create_tuple();
        while (target.consume(tuple) != DFI_FLOW_FINISHED)
        {
            consumed_params_1.push_back(tuple.getAs<uint64_t>("value"));
        }
    });
    // ------- Flow Target 2 -------
    std::vector<uint64_t> consumed_params_2;
    std::thread target_thread2([&]() {
        DFI_Replicate_flow_target target(flow_name, 2);
        dfi::Tuple tuple = target.create_tuple();
        while (target.consume(tuple) != DFI_FLOW_FINISHED)
        {
            consumed_params_2.push_back(tuple.getAs<uint64_t>("value"));
        }
    });
    // ------- Flow Source 1 -------
    std::thread source_thread1([&]() {
        DFI_Replicate_flow_source source(flow_name, 1);
        for (uint64_t i = 0; i < tuple_count; i++)
        {
            uint64_t raw_tuple = i;
            source.push(&raw_tuple);
        }
    });


    source_thread1.join();
    target_thread1.join();
    target_thread2.join();

    EXPECT_EQ(tuple_count, consumed_params_1.size()) << "Size of consumed tuples for Flow Target 1 does not match expected.";
    ASSERT_EQ(tuple_count, consumed_params_2.size()) << "Size of consumed tuples for Flow Target 2 does not match expected.";
    
    size_t i = 0;
    for (auto key : consumed_params_1) {
        ASSERT_EQ(i++, key) << "Consumed tuple for Flow Target 1 does not match expected.";
    }

    i = 0;
    for (auto key : consumed_params_2) {
        ASSERT_EQ(i++, key) << "Consumed tuple for Flow Target 2 does not match expected.";
    }
}


//Tests more tuple_count than segments in ring, i.e. that queues wraps correctly around and push blocks  
TEST_F(TestReplicateFlow, multicast_2src_2tgt_BW)
{
    Config::DFI_NODES.push_back(rdma::Config::getIP(rdma::Config::RDMA_INTERFACE) + ":" + to_string(Config::DFI_NODE_PORT+2)); //Node for source 1
    Config::DFI_NODES.push_back(rdma::Config::getIP(rdma::Config::RDMA_INTERFACE) + ":" + to_string(Config::DFI_NODE_PORT+3)); //Node for source 2
    
    Config::DFI_SEGMENTS_PER_RING = 10;
    string flow_name = "Multicast Replicate Flow";
    DFI_Schema schema({{"value", dfi::TypeId::BIGINT},{"value2", dfi::TypeId::BIGINT},{"3", dfi::TypeId::BIGINT},{"4", dfi::TypeId::BIGINT},{"5", dfi::TypeId::BIGINT},{"6", dfi::TypeId::BIGINT},{"7", dfi::TypeId::BIGINT},{"8", dfi::TypeId::BIGINT}});
    std::vector<source_t> sources = {{1, 3}, {2, 4}};
    std::vector<target_t> targets = {{1, 0}, {2, 0}};

    DFI_Replicate_flow_init(flow_name, sources, targets, schema, DFI_Replicate_flow_optimization::BandwidthMulticast);
    size_t tuple_count = 10000;

    // ------- Flow Source 1 -------
    std::thread source_thread1([&]() {
        DFI_Replicate_flow_source source{flow_name, 1};
        for (uint64_t i = 0; i < tuple_count/2; i++)
        {
            uint64_t raw_tuple = i;
            source.push(&raw_tuple);
        }
    });

    // ------- Flow Source 2 -------
    std::thread source_thread2([&]() {
        DFI_Replicate_flow_source source{flow_name, 2};
        for (uint64_t i = tuple_count/2; i < tuple_count; i++)
        {
            uint64_t raw_tuple = i;
            source.push(&raw_tuple);
        }
    });

    // ------- Flow Target 1 -------
    std::vector<uint64_t> consumed_tuples1;
    std::thread target_thread1([&]() {
        DFI_Replicate_flow_target target{flow_name, 1};
        dfi::Tuple tuple = target.create_tuple();
        int ret;
        while ((ret = target.consume(tuple)) != DFI_FLOW_FINISHED)
        {
            ASSERT_EQ(ret, DFI_SUCCESS);
            consumed_tuples1.push_back(tuple.getAs<uint64_t>("value"));
        }
    });

    // ------- Flow Target 2 -------
    std::vector<uint64_t> consumed_tuples2;
    std::thread target_thread2([&]() {
        DFI_Replicate_flow_target target{flow_name, 2};
        dfi::Tuple tuple = target.create_tuple();
        int ret;
        size_t tuple_cnt;
        while ((ret = target.consume(tuple, tuple_cnt)) != DFI_FLOW_FINISHED)
        {
            ASSERT_EQ(ret, DFI_SUCCESS);
            for (size_t i = 0; i < tuple_cnt; i++)
            {
                consumed_tuples2.push_back(tuple.getAs<uint64_t>("value"));
                tuple.setDataPtr(tuple.getDataPtr() + schema.getTupleSize());
            }
            
        }
    });
    
    source_thread1.join();
    source_thread2.join();
    target_thread1.join();
    target_thread2.join();

    ASSERT_EQ(tuple_count, consumed_tuples1.size()) << "Size of consumed tuples for Flow Target does not match expected.";
    ASSERT_EQ(tuple_count, consumed_tuples2.size()) << "Size of consumed tuples for Flow Target does not match expected.";

    ASSERT_EQ(consumed_tuples1, consumed_tuples2); //check for ordering
    // size_t i = 0;
    // for (auto key : consumed_tuples1) {
    //     ASSERT_EQ(i++, key) << "Consumed tuple for Flow Target 1 does not match expected.";
    // }
    // i = 0;
    // for (auto key : consumed_tuples2) {
    //     ASSERT_EQ(i++, key) << "Consumed tuple for Flow Target 2 does not match expected.";
    // }
}

//Tests more tuple_count than segments in ring, i.e. that queues wraps correctly around and push blocks  
TEST_F(TestReplicateFlow, multicast_1src_1tgt_BW)
{
    Config::DFI_NODES.push_back(rdma::Config::getIP(rdma::Config::RDMA_INTERFACE) + ":" + to_string(Config::DFI_NODE_PORT+2)); //Node for source 1
    
    Config::DFI_SEGMENTS_PER_RING = 100;
    string flow_name = "Multicast Replicate Flow";
    DFI_Schema schema({{"value", dfi::TypeId::BIGINT},{"value2", dfi::TypeId::BIGINT},{"3", dfi::TypeId::BIGINT},{"4", dfi::TypeId::BIGINT},{"5", dfi::TypeId::BIGINT},{"6", dfi::TypeId::BIGINT},{"7", dfi::TypeId::BIGINT},{"8", dfi::TypeId::BIGINT}});
    std::vector<source_t> sources = {{1, 3}};
    std::vector<target_t> targets = {{1, 0}};

    DFI_Replicate_flow_init(flow_name, sources, targets, schema, DFI_Replicate_flow_optimization::BandwidthMulticast);
    size_t tuple_count = 10000;

    // ------- Flow Source 1 -------
    std::thread source_thread1([&]() {
        DFI_Replicate_flow_source source{flow_name, 1};
        for (uint64_t i = 0; i < tuple_count; i++)
        {
            uint64_t raw_tuple[8] = {i};
            source.push(raw_tuple);
        }
    });


    // ------- Flow Target 1 -------
    std::vector<uint64_t> consumed_tuples1;
    std::thread target_thread1([&]() {
        DFI_Replicate_flow_target target{flow_name, 1};
        dfi::Tuple tuple = target.create_tuple();
        int ret;
        size_t tuple_cnt;
        while ((ret = target.consume(tuple, tuple_cnt)) != DFI_FLOW_FINISHED)
        {
            ASSERT_EQ(ret, DFI_SUCCESS);
            for (size_t i = 0; i < tuple_cnt; i++)
            {
                consumed_tuples1.push_back(tuple.getAs<uint64_t>("value"));
                tuple.setDataPtr(tuple.getDataPtr() + schema.getTupleSize());
            }
            
        }
    });
    
    source_thread1.join();
    target_thread1.join();

    ASSERT_EQ(tuple_count, consumed_tuples1.size()) << "Size of consumed tuples for Flow Target does not match expected.";

    size_t i = 0;
    for (auto key : consumed_tuples1) {
        ASSERT_EQ(i++, key) << "Consumed tuple for Flow Target 1 does not match expected.";
    }
}

TEST_F(TestReplicateFlow, multicast_1src_1tgt)
{
    Config::DFI_NODES.push_back(rdma::Config::getIP(rdma::Config::RDMA_INTERFACE) + ":" + to_string(Config::DFI_NODE_PORT+2)); //Node for source
    
    Config::DFI_SEGMENTS_PER_RING = 150;
    string flow_name = "Multicast Replicate Flow";
    DFI_Schema schema({{"value", dfi::TypeId::BIGINT}});
    std::vector<source_t> sources = {{1, 3}};
    std::vector<target_t> targets = {{1, 0}};

    DFI_Replicate_flow_init(flow_name, sources, targets, schema, DFI_Replicate_flow_optimization::LatencyOrderedMulticast);

    size_t tuple_count = 10;

    // ------- Flow Source 1 -------
    std::thread source_thread1([&]() {
        DFI_Replicate_flow_source source{flow_name, 1};
        for (uint64_t i = 0; i < tuple_count; i++)
        {
            uint64_t raw_tuple = i;
            source.push(&raw_tuple);
        }
        source.close();
    });
    
    // ------- Flow Target 1 -------
    std::vector<uint64_t> consumed_tuples;
    std::thread target_thread1([&]() {
        DFI_Replicate_flow_target target{flow_name, 1};
        dfi::Tuple tuple = target.create_tuple();
        while (target.consume(tuple) != DFI_FLOW_FINISHED)
        {
            consumed_tuples.push_back(tuple.getAs<uint64_t>("value"));
        }
    });
    
    source_thread1.join();
    target_thread1.join();

    ASSERT_EQ(tuple_count, consumed_tuples.size()) << "Size of consumed tuples for Flow Target 1 does not match expected.";

    size_t i = 0;
    for (auto key : consumed_tuples) {
        ASSERT_EQ(i++, key) << "Consumed tuple for Flow Target 1 does not match expected.";
    }
}


TEST_F(TestReplicateFlow, multicast_1src_1tgt_2grps)
{
    Config::DFI_MULTICAST_ADDRS = {
        "172.18.94.10", "172.18.94.11",
        "172.18.94.20", "172.18.94.21",
        "172.18.94.30", "172.18.94.31",
        "172.18.94.40", "172.18.94.41",
        "172.18.94.50", "172.18.94.51",
        "172.18.94.60", "172.18.94.61",
        "172.18.94.70", "172.18.94.71",
        "172.18.94.80", "172.18.94.81",
    };
    Config::DFI_NODES.push_back(rdma::Config::getIP(rdma::Config::RDMA_INTERFACE) + ":" + to_string(Config::DFI_NODE_PORT+2)); //Node for source
    Config::DFI_NODES.push_back(rdma::Config::getIP(rdma::Config::RDMA_INTERFACE) + ":" + to_string(Config::DFI_NODE_PORT+3)); //Node for source, grp 2
    
    Config::DFI_SEGMENTS_PER_RING = 150;
    string flow_name = "Multicast Replicate Flow";
    string flow_name2 = "Multicast Replicate Flow2";
    DFI_Schema schema({{"value", dfi::TypeId::BIGINT}});
    std::vector<source_t> sources = {{1, 3}};
    std::vector<target_t> targets = {{1, 0}};

    DFI_Replicate_flow_init(flow_name, sources, targets, schema, DFI_Replicate_flow_optimization::LatencyOrderedMulticast);
    sources = {{1, 4}}; //grp 2 sources
    DFI_Replicate_flow_init(flow_name2, sources, targets, schema, DFI_Replicate_flow_optimization::LatencyOrderedMulticast);

    size_t tuple_count = 10;


    // ---------------------- GROUP 1 ----------------------
    // ------- Flow Source 1 -------
    std::thread source_thread1([&]() {
        DFI_Replicate_flow_source source{flow_name, 1};
        for (uint64_t i = 0; i < tuple_count; i++)
        {
            uint64_t raw_tuple = i;
            source.push(&raw_tuple);
        }
        source.close();
    });
    
    // ------- Flow Target 1 -------
    std::vector<uint64_t> consumed_tuples;
    std::thread target_thread1([&]() {
        DFI_Replicate_flow_target target{flow_name, 1};
        dfi::Tuple tuple = target.create_tuple();
        while (target.consume(tuple) != DFI_FLOW_FINISHED)
        {
            consumed_tuples.push_back(tuple.getAs<uint64_t>("value"));
        }
    });
    


    // ---------------------- GROUP 2 ----------------------
    // ------- Flow Source 1 -------
    std::thread source_thread2([&]() {
        DFI_Replicate_flow_source source{flow_name2, 1};
        for (uint64_t i = 0; i < tuple_count; i++)
        {
            uint64_t raw_tuple = i;
            source.push(&raw_tuple);
        }
        source.close();
    });
    
    // ------- Flow Target 1 -------
    std::vector<uint64_t> consumed_tuples2;
    std::thread target_thread2([&]() {
        DFI_Replicate_flow_target target{flow_name2, 1};
        dfi::Tuple tuple = target.create_tuple();
        while (target.consume(tuple) != DFI_FLOW_FINISHED)
        {
            consumed_tuples2.push_back(tuple.getAs<uint64_t>("value"));
        }
    });
    
    source_thread1.join();
    target_thread1.join();
    source_thread2.join();
    target_thread2.join();

    ASSERT_EQ(tuple_count, consumed_tuples.size()) << "Size of consumed tuples for Flow Target 1 does not match expected.";

    size_t i = 0;
    for (auto key : consumed_tuples) {
        ASSERT_EQ(i++, key) << "Consumed tuple for Flow Target 1 does not match expected.";
    }
    ASSERT_EQ(tuple_count, consumed_tuples2.size()) << "Size of consumed tuples for Flow Target 1 does not match expected.";

    i = 0;
    for (auto key : consumed_tuples2) {
        ASSERT_EQ(i++, key) << "Consumed tuple for Flow Target 1 does not match expected.";
    }
}



//Tests more tuple_count than segments in ring, i.e. that queues wraps correctly around and push blocks  
TEST_F(TestReplicateFlow, multicast_1src_2tgt_wraparound)
{
    Config::DFI_NODES.push_back(rdma::Config::getIP(rdma::Config::RDMA_INTERFACE) + ":" + to_string(Config::DFI_NODE_PORT+2)); //Node for source
    
    Config::DFI_SEGMENTS_PER_RING = 50;
    string flow_name = "Multicast Replicate Flow";
    DFI_Schema schema({{"value", dfi::TypeId::BIGINT}});
    std::vector<source_t> sources = {{1, 3}};
    std::vector<target_t> targets = {{1, 0}, {2, 0}};

    DFI_Replicate_flow_init(flow_name, sources, targets, schema, DFI_Replicate_flow_optimization::LatencyOrderedMulticast);

    size_t tuple_count = 100000;

    // ------- Flow Source 1 -------
    std::thread source_thread1([&]() {
        DFI_Replicate_flow_source source{flow_name, 1};
        for (uint64_t i = 0; i < tuple_count; i++)
        {
            uint64_t raw_tuple = i;
            source.push(&raw_tuple);
        }
    });

    // ------- Flow Target 1 -------
    std::vector<uint64_t> consumed_tuples1;
    std::thread target_thread1([&]() {
        DFI_Replicate_flow_target target{flow_name, 1};
        dfi::Tuple tuple = target.create_tuple();
        int ret;
        while ((ret = target.consume(tuple)) != DFI_FLOW_FINISHED)
        {
            if (ret == DFI_MESSAGE_LOST)
            {
                std::cout << "detected loss" << std::endl;
            }
            consumed_tuples1.push_back(tuple.getAs<uint64_t>("value"));
        }
    });

    // ------- Flow Target 2 -------
    std::vector<uint64_t> consumed_tuples2;
    std::thread target_thread2([&]() {
        DFI_Replicate_flow_target target{flow_name, 2};
        dfi::Tuple tuple = target.create_tuple();
        int ret;
        while ((ret = target.consume(tuple)) != DFI_FLOW_FINISHED)
        {
            if (ret == DFI_MESSAGE_LOST)
            {
                std::cout << "detected loss" << std::endl;
            }
            consumed_tuples2.push_back(tuple.getAs<uint64_t>("value"));
        }
    });
    
    source_thread1.join();
    target_thread1.join();
    target_thread2.join();

    ASSERT_EQ(tuple_count, consumed_tuples1.size()) << "Size of consumed tuples for Flow Target does not match expected.";
    ASSERT_EQ(tuple_count, consumed_tuples2.size()) << "Size of consumed tuples for Flow Target does not match expected.";

    ASSERT_EQ(consumed_tuples1, consumed_tuples2);
}


//Tests more tuple_count than segments in ring, i.e. that queues wraps correctly around and push blocks  
TEST_F(TestReplicateFlow, multicast_2src_2tgt_wraparound)
{
    Config::DFI_NODES.push_back(rdma::Config::getIP(rdma::Config::RDMA_INTERFACE) + ":" + to_string(Config::DFI_NODE_PORT+2)); //Node for source 1
    Config::DFI_NODES.push_back(rdma::Config::getIP(rdma::Config::RDMA_INTERFACE) + ":" + to_string(Config::DFI_NODE_PORT+3)); //Node for source 2
    
    Config::DFI_SEGMENTS_PER_RING = 20;
    string flow_name = "Multicast Replicate Flow";
    DFI_Schema schema({{"value", dfi::TypeId::BIGINT}});
    std::vector<source_t> sources = {{1, 3}, {2, 4}};
    std::vector<target_t> targets = {{1, 0}, {2, 0}};

    DFI_Replicate_flow_init(flow_name, sources, targets, schema, DFI_Replicate_flow_optimization::LatencyOrderedMulticast);

    size_t tuple_count = 10000;

    // ------- Flow Source 1 -------
    std::thread source_thread1([&]() {
        DFI_Replicate_flow_source source{flow_name, 1};
        for (uint64_t i = 0; i < tuple_count/2; i++)
        {
            uint64_t raw_tuple = i;
            source.push(&raw_tuple);
        }
    });

    // ------- Flow Source 2 -------
    std::thread source_thread2([&]() {
        DFI_Replicate_flow_source source{flow_name, 2};
        for (uint64_t i = tuple_count/2; i < tuple_count; i++)
        {
            uint64_t raw_tuple = i;
            source.push(&raw_tuple);
        }
    });

    // ------- Flow Target 1 -------
    std::vector<uint64_t> consumed_tuples1;
    std::thread target_thread1([&]() {
        DFI_Replicate_flow_target target{flow_name, 1};
        dfi::Tuple tuple = target.create_tuple();
        int ret;
        while ((ret = target.consume(tuple)) != DFI_FLOW_FINISHED)
        {
            if (ret == DFI_MESSAGE_LOST)
                consumed_tuples1.push_back(0); //dummy insert to allow lost messages (no reliability guarantee)
            else
                consumed_tuples1.push_back(tuple.getAs<uint64_t>("value"));
        }
    });

    // ------- Flow Target 2 -------
    std::vector<uint64_t> consumed_tuples2;
    std::thread target_thread2([&]() {
        DFI_Replicate_flow_target target{flow_name, 2};
        dfi::Tuple tuple = target.create_tuple();
        int ret;
        while ((ret = target.consume(tuple)) != DFI_FLOW_FINISHED)
        {
            if (ret == DFI_MESSAGE_LOST)
                consumed_tuples2.push_back(0); //dummy insert to allow lost messages (no reliability guarantee)
            else
                consumed_tuples2.push_back(tuple.getAs<uint64_t>("value"));
        }
    });
    
    source_thread1.join();
    source_thread2.join();
    target_thread1.join();
    target_thread2.join();

    ASSERT_EQ(tuple_count, consumed_tuples1.size()) << "Size of consumed tuples for Flow Target does not match expected.";
    ASSERT_EQ(tuple_count, consumed_tuples2.size()) << "Size of consumed tuples for Flow Target does not match expected.";

    ASSERT_EQ(consumed_tuples1, consumed_tuples2); //check for ordering
    // size_t i = 0;
    // for (auto key : consumed_tuples1) {
    //     ASSERT_EQ(i++, key) << "Consumed tuple for Flow Target 1 does not match expected.";
    // }
    // i = 0;
    // for (auto key : consumed_tuples2) {
    //     ASSERT_EQ(i++, key) << "Consumed tuple for Flow Target 2 does not match expected.";
    // }
}

//Tests more tuple_count than segments in ring, i.e. that queues wraps correctly around and push blocks  
TEST_F(TestReplicateFlow, multicast_2src_1tgt_slow_source)
{
    Config::DFI_NODES.push_back(rdma::Config::getIP(rdma::Config::RDMA_INTERFACE) + ":" + to_string(Config::DFI_NODE_PORT+2)); //Node for source 1
    Config::DFI_NODES.push_back(rdma::Config::getIP(rdma::Config::RDMA_INTERFACE) + ":" + to_string(Config::DFI_NODE_PORT+3)); //Node for source 2
    
    Config::DFI_SEGMENTS_PER_RING = 50;
    string flow_name = "Multicast Replicate Flow";
    DFI_Schema schema({{"value", dfi::TypeId::BIGINT}});
    std::vector<source_t> sources = {{1, 3}, {2, 4}};
    std::vector<target_t> targets = {{1, 0}};

    DFI_Replicate_flow_init(flow_name, sources, targets, schema, DFI_Replicate_flow_optimization::LatencyOrderedMulticast);

    size_t tuple_count = 10000;

    // ------- Flow Source 1 -------
    std::thread source_thread1([&]() {
        DFI_Replicate_flow_source source{flow_name, 1};
        for (uint64_t i = 0; i < tuple_count/2; i++)
        {
            uint64_t raw_tuple = i;
            source.push(&raw_tuple);
        }
    });

    // ------- Flow Source 2 -------
    std::thread source_thread2([&]() {
        DFI_Replicate_flow_source source{flow_name, 2};
        sleep(1); //Test that source can push even though global_seq_no has moved 
        for (uint64_t i = tuple_count/2; i < tuple_count; i++)
        {
            uint64_t raw_tuple = i;
            source.push(&raw_tuple);
            usleep(10);
        }
    });

    // ------- Flow Target 1 -------
    std::vector<uint64_t> consumed_tuples1;
    std::thread target_thread1([&]() {
        DFI_Replicate_flow_target target{flow_name, 1};
        dfi::Tuple tuple = target.create_tuple();
        int ret;
        while ((ret = target.consume(tuple)) != DFI_FLOW_FINISHED)
        {
            if (ret == DFI_MESSAGE_LOST)
                consumed_tuples1.push_back(0); //dummy insert to allow lost messages (no reliability guarantee)
            else
                consumed_tuples1.push_back(tuple.getAs<uint64_t>("value"));
        }
    });

    source_thread1.join();
    source_thread2.join();
    target_thread1.join();

    ASSERT_EQ(tuple_count, consumed_tuples1.size()) << "Size of consumed tuples for Flow Target 1 does not match expected.";
    std::sort(consumed_tuples1.begin(), consumed_tuples1.end());
    size_t i = 0;
    for (auto key : consumed_tuples1) {
        ASSERT_EQ(i++, key) << "Consumed tuple for Flow Target 1 does not match expected.";
    }
}


//Tests more tuple_count than segments in ring, i.e. that queues wraps correctly around and push blocks  
TEST_F(TestReplicateFlow, multicast_2src_1tgt_slow_target)
{
    Config::DFI_NODES.push_back(rdma::Config::getIP(rdma::Config::RDMA_INTERFACE) + ":" + to_string(Config::DFI_NODE_PORT+2)); //Node for source 1
    Config::DFI_NODES.push_back(rdma::Config::getIP(rdma::Config::RDMA_INTERFACE) + ":" + to_string(Config::DFI_NODE_PORT+3)); //Node for source 2
    
    Config::DFI_SEGMENTS_PER_RING = 100;
    string flow_name = "Multicast Replicate Flow";
    DFI_Schema schema({{"value", dfi::TypeId::BIGINT}});
    std::vector<source_t> sources = {{1, 3}, {2, 4}};
    std::vector<target_t> targets = {{1, 0}};

    DFI_Replicate_flow_init(flow_name, sources, targets, schema, DFI_Replicate_flow_optimization::LatencyOrderedMulticast);

    size_t tuple_count = 10000;

    // ------- Flow Source 1 -------
    std::thread source_thread1([&]() {
        DFI_Replicate_flow_source source{flow_name, 1};
        for (uint64_t i = 0; i < tuple_count/2; i++)
        {
            uint64_t raw_tuple = i;
            source.push(&raw_tuple);
        }
    });

    // ------- Flow Source 2 -------
    std::thread source_thread2([&]() {
        DFI_Replicate_flow_source source{flow_name, 2};
        for (uint64_t i = tuple_count/2; i < tuple_count; i++)
        {
            uint64_t raw_tuple = i;
            source.push(&raw_tuple);
        }
    });

    // ------- Flow Target 1 -------
    std::vector<uint64_t> consumed_tuples1;
    std::thread target_thread1([&]() {
        DFI_Replicate_flow_target target{flow_name, 1};
        dfi::Tuple tuple = target.create_tuple();
        int ret;
        while ((ret = target.consume(tuple)) != DFI_FLOW_FINISHED)
        {
            if (ret == DFI_MESSAGE_LOST)
                consumed_tuples1.push_back(0); //dummy insert to allow lost messages (no reliability guarantee)
            else
                consumed_tuples1.push_back(tuple.getAs<uint64_t>("value"));
            usleep(10);
        }
    });

    source_thread1.join();
    source_thread2.join();
    target_thread1.join();

    ASSERT_EQ(tuple_count, consumed_tuples1.size()) << "Size of consumed tuples for Flow Target 1 does not match expected.";
    
    // std::sort(consumed_tuples1.begin(), consumed_tuples1.end());
    // size_t i = 0;
    // for (auto key : consumed_tuples1) {
    //     ASSERT_EQ(i++, key) << "Consumed tuple for Flow Target 1 does not match expected.";
    // }
}


TEST_F(TestReplicateFlow, multicast_2src_1tgt)
{
    Config::DFI_NODES.push_back(rdma::Config::getIP(rdma::Config::RDMA_INTERFACE) + ":" + to_string(Config::DFI_NODE_PORT+2)); //Node for source 1
    Config::DFI_NODES.push_back(rdma::Config::getIP(rdma::Config::RDMA_INTERFACE) + ":" + to_string(Config::DFI_NODE_PORT+3)); //Node for source 2
    
    Config::DFI_SEGMENTS_PER_RING = 50;
    string flow_name = "Multicast Replicate Flow";
    DFI_Schema schema({{"value", dfi::TypeId::BIGINT}});
    std::vector<source_t> sources = {{1, 3}, {2, 4}};
    std::vector<target_t> targets = {{1, 0}};

    DFI_Replicate_flow_init(flow_name, sources, targets, schema, DFI_Replicate_flow_optimization::LatencyOrderedMulticast);

    size_t tuple_count = 1000;

    // ------- Flow Source 1 -------
    std::thread source_thread1([&]() {
        DFI_Replicate_flow_source source{flow_name, 1};
        for (uint64_t i = 0; i < tuple_count/2; i++)
        {
            uint64_t raw_tuple = i;
            source.push(&raw_tuple);
        }
    });

    // ------- Flow Source 2 -------
    std::thread source_thread2([&]() {
        DFI_Replicate_flow_source source{flow_name, 2};
        for (uint64_t i = tuple_count/2; i < tuple_count; i++)
        {
            uint64_t raw_tuple = i;
            source.push(&raw_tuple);
        }
    });

    // ------- Flow Target 1 -------
    std::vector<uint64_t> consumed_tuples1;
    std::thread target_thread1([&]() {
        DFI_Replicate_flow_target target{flow_name, 1};
        dfi::Tuple tuple = target.create_tuple();
        int ret;
        while ((ret = target.consume(tuple)) != DFI_FLOW_FINISHED)
        {
            if (ret == DFI_MESSAGE_LOST)
                consumed_tuples1.push_back(0); //dummy insert to counter lost messages (no reliability guarantee)
            else
                consumed_tuples1.push_back(tuple.getAs<uint64_t>("value"));
        }
    });

    source_thread1.join();
    source_thread2.join();
    target_thread1.join();

    ASSERT_EQ(tuple_count, consumed_tuples1.size()) << "Size of consumed tuples for Flow Target 1 does not match expected.";
    
}

//Tests more tuple_count than segments in ring, i.e. that queues wraps correctly around and push blocks  
TEST_F(TestReplicateFlow, multicast_12src_12tgt_wraparound)
{
    size_t count = 12;
    std::vector<source_t> sources;
    std::vector<target_t> targets;
    for (size_t i = 0; i < count; i++)
    {
        Config::DFI_NODES.push_back(rdma::Config::getIP(rdma::Config::RDMA_INTERFACE) + ":" + to_string(Config::DFI_NODE_PORT+2+i)); //Nodes for sources   
        sources.push_back({i+1, i+3});
        targets.push_back({i+1, 0});
    }
    
    Config::DFI_SEGMENTS_PER_RING = 4096;
    string flow_name = "Multicast Replicate Flow";
    DFI_Schema schema({{"value", dfi::TypeId::BIGINT}});

    DFI_Replicate_flow_init(flow_name, sources, targets, schema, DFI_Replicate_flow_optimization::LatencyOrderedMulticast);

    size_t tuple_count = count*10000;

    std::vector<std::thread> threads;
    std::vector<std::vector<uint64_t>> consumed_tuples;
    consumed_tuples.resize(count);

    for (size_t i = 0; i < count; i++)
    {
        // ------- Flow Sources -------
        threads.emplace_back([&count, i, tuple_count, &flow_name]() {
            DFI_Replicate_flow_source source{flow_name, i+1};
            for (uint64_t j = 0; j < tuple_count/count; j++)
            {
                uint64_t raw_tuple = j + i * (tuple_count/count);
                source.push(&raw_tuple);
            }
        });

        threads.emplace_back([&count, tuple_count, &flow_name, &consumed_tuples, i]() {
            DFI_Replicate_flow_target target{flow_name, 1+i};
            dfi::Tuple tuple = target.create_tuple();
            int ret;
            while ((ret = target.consume(tuple)) != DFI_FLOW_FINISHED)
            {
                if (ret == DFI_MESSAGE_LOST)
                {
                    std::cout << "lost message" << std::endl;
                    consumed_tuples[i].push_back(0);
                }
                else
                {
                    consumed_tuples[i].push_back(tuple.getAs<uint64_t>("value"));                    
                }
                
            }
        });
    }


    for (size_t i = 0; i < count*2; i++)
    {
        threads[i].join();
    }
    for (size_t i = 0; i < count; i++)
    {
        ASSERT_EQ(tuple_count, consumed_tuples[i].size()) << "Size of consumed tuples for Flow Target does not match expected.";
        
        ASSERT_EQ(consumed_tuples[0], consumed_tuples[i]);

        // std::sort(consumed_tuples[i].begin(), consumed_tuples[i].end());

        // size_t j = 0;
        // for (auto key : consumed_tuples[i]) {
        //     ASSERT_EQ(j++ , key) << "Consumed tuple for Flow Target does not match expected.";
        // }
    }
}

TEST_F(TestReplicateFlow, multicast_12src_1tgt_wraparound)
{
    size_t count = 12;
    std::vector<source_t> sources;
    std::vector<target_t> targets;
    targets.push_back({1, 0});
    for (size_t i = 0; i < count; i++)
    {
        Config::DFI_NODES.push_back(rdma::Config::getIP(rdma::Config::RDMA_INTERFACE) + ":" + to_string(Config::DFI_NODE_PORT+2+i)); //Nodes for sources   
        sources.push_back({i+1, i+3});
    }
    
    Config::DFI_SEGMENTS_PER_RING = 4096;
    string flow_name = "Multicast Replicate Flow";
    DFI_Schema schema({{"value", dfi::TypeId::BIGINT}});

    DFI_Replicate_flow_init(flow_name, sources, targets, schema, DFI_Replicate_flow_optimization::LatencyOrderedMulticast);

    size_t tuple_count = count*1000;

    std::vector<std::thread> threads;
    std::vector<uint64_t> consumed_tuples;

    for (size_t i = 0; i < count; i++)
    {
        // ------- Flow Sources -------
        threads.emplace_back([&count, i, tuple_count, &flow_name]() {
            DFI_Replicate_flow_source source{flow_name, i+1};
            for (uint64_t j = 0; j < tuple_count/count; j++)
            {
                uint64_t raw_tuple = j + i * (tuple_count/count);
                source.push(&raw_tuple);
            }
        });

    }

    threads.emplace_back([&count, tuple_count, &flow_name, &consumed_tuples]() {
        DFI_Replicate_flow_target target{flow_name, 1};
        size_t lost_tuples = 0;
        dfi::Tuple tuple = target.create_tuple();
        int ret;
        while ((ret = target.consume(tuple)) != DFI_FLOW_FINISHED)
        {
            if (ret == DFI_MESSAGE_LOST)
            {
                std::cout << "lost message" << std::endl;
                ++lost_tuples;
                consumed_tuples.push_back(0);
            }
            else
            {
                consumed_tuples.push_back(tuple.getAs<uint64_t>("value"));                    
            }

        }
        std::cout << "lost_tuples: " << lost_tuples << std::endl;
    });

    for (size_t i = 0; i < count+1; i++)
    {
        threads[i].join();
    }

    ASSERT_EQ(tuple_count, consumed_tuples.size()) << "Size of consumed tuples for Flow Target does not match expected.";
    
}


//Tests more tuple_count than segments in ring, i.e. that queues wraps correctly around and push blocks  
TEST_F(TestReplicateFlow, multicast_1src_12tgt)
{
    size_t count = 12;
    std::vector<source_t> sources;
    std::vector<target_t> targets;

    Config::DFI_NODES.push_back(rdma::Config::getIP(rdma::Config::RDMA_INTERFACE) + ":" + to_string(Config::DFI_NODE_PORT+2)); //Nodes for sources   
    sources.push_back({1, 3});
    for (size_t i = 0; i < count; i++)
    {
        targets.push_back({i+1, 0});
    }
    
    Config::DFI_SEGMENTS_PER_RING = 4096;
    string flow_name = "Multicast Replicate Flow";
    DFI_Schema schema({{"value", dfi::TypeId::BIGINT}});

    DFI_Replicate_flow_init(flow_name, sources, targets, schema, DFI_Replicate_flow_optimization::LatencyOrderedMulticast);

    size_t tuple_count = count*10000;

    std::vector<std::thread> threads;
    std::vector<std::vector<uint64_t>> consumed_tuples;
    consumed_tuples.resize(count);

    // ------- Flow Source -------
    threads.emplace_back([tuple_count, &flow_name]() {
        DFI_Replicate_flow_source source{flow_name, 1};
        for (uint64_t j = 0; j < tuple_count; j++)
        {
            uint64_t raw_tuple = j;
            source.push(&raw_tuple);
        }
    });
    for (size_t i = 0; i < count; i++)
    {
        threads.emplace_back([&count, tuple_count, &flow_name, &consumed_tuples, i]() {
            DFI_Replicate_flow_target target{flow_name, 1+i};
            dfi::Tuple tuple = target.create_tuple();
            int ret;
            while ((ret = target.consume(tuple)) != DFI_FLOW_FINISHED)
            {
                if (ret == DFI_MESSAGE_LOST)
                {
                    std::cout << "lost message" << std::endl;
                    consumed_tuples[i].push_back(0);
                }
                else
                {
                    consumed_tuples[i].push_back(tuple.getAs<uint64_t>("value"));                    
                }
                
            }
        });
    }


    for (size_t i = 0; i < count+1; i++)
    {
        threads[i].join();
    }
    for (size_t i = 0; i < count; i++)
    {
        ASSERT_EQ(tuple_count, consumed_tuples[i].size()) << "Size of consumed tuples for Flow Target does not match expected.";
        
        ASSERT_EQ(consumed_tuples[0], consumed_tuples[i]);

        std::sort(consumed_tuples[i].begin(), consumed_tuples[i].end());

        size_t j = 0;
        for (auto key : consumed_tuples[i]) {
            ASSERT_EQ(j++ , key) << "Consumed tuple for Flow Target does not match expected.";
        }
    }
}



TEST_F(TestReplicateFlow, multicast_size_exceeds_MTU)
{
    string flow_name = "Multicast Replicate Flow";

    std::vector<std::pair<std::string, TypeId>> columns;

    for (size_t i = 0; i < rdma::Config::RDMA_UD_MTU-sizeof(DFI_MULTICAST_SEGMENT_HEADER_t)+1; i++)
    {
        columns.push_back({"value"+to_string(i), dfi::TypeId::TINYINT});
    }
    
    DFI_Schema schema(columns);
    std::vector<source_t> sources = {{1, 3}};
    std::vector<target_t> targets = {{1, 0}, {2, 0}};
    EXPECT_THROW(DFI_Replicate_flow_init(flow_name, sources, targets, schema, DFI_Replicate_flow_optimization::LatencyOrderedMulticast), std::invalid_argument);
}

TEST_F(TestReplicateFlow, multicast_1src_2tgt_large_tuple)
{
    Config::DFI_NODES.push_back(rdma::Config::getIP(rdma::Config::RDMA_INTERFACE) + ":" + to_string(Config::DFI_NODE_PORT+2)); //Node for source
    Config::DFI_SEGMENTS_PER_RING = 10;
    string flow_name = "Multicast Replicate Flow";

    std::vector<std::pair<std::string, TypeId>> columns;

    for (size_t i = 0; i < 4096-sizeof(DFI_MULTICAST_SEGMENT_HEADER_t); i++)
    {
        columns.push_back({"value"+to_string(i), dfi::TypeId::TINYINT});
    }

    
    // ------- MASTER -------
    DFI_Schema schema(columns);
    std::vector<source_t> sources = {{1, 3}};
    std::vector<target_t> targets = {{1, 0}, {2, 0}};
    DFI_Replicate_flow_init(flow_name, sources, targets, schema, DFI_Replicate_flow_optimization::BandwidthMulticast);
    size_t tuple_count = 100;
    // ------- Flow Source 1 -------
    std::thread source_thread1([&]() {
        DFI_Replicate_flow_source source(flow_name, 1);
        uint8_t raw_tuple[4096] = {0};
        for (uint8_t i = 0; i < tuple_count; i++)
        {
            raw_tuple[0] = i;
            source.push(raw_tuple);
        }
    });

    // ------- Flow Target 1 -------
    std::vector<uint8_t> consumed_params_1;
    std::thread target_thread1([&]() {
        DFI_Replicate_flow_target target(flow_name, 1);
        dfi::Tuple tuple = target.create_tuple();
        int ret;
            while ((ret = target.consume(tuple)) != DFI_FLOW_FINISHED)
            {
                if (ret == DFI_MESSAGE_LOST)
                {
                    std::cout << "lost message" << std::endl;
                    consumed_params_1.push_back(0);
                }
                else
                {
                    consumed_params_1.push_back(tuple.getAs<uint64_t>("value0"));                    
                }
            }
    });
    // ------- Flow Target 2 -------
    std::vector<uint8_t> consumed_params_2;
    std::thread target_thread2([&]() {
        DFI_Replicate_flow_target target(flow_name, 2);
        dfi::Tuple tuple = target.create_tuple();
        int ret;
            while ((ret = target.consume(tuple)) != DFI_FLOW_FINISHED)
            {
                if (ret == DFI_MESSAGE_LOST)
                {
                    std::cout << "lost message" << std::endl;
                    consumed_params_2.push_back(0);
                }
                else
                {
                    consumed_params_2.push_back(tuple.getAs<uint64_t>("value0"));                    
                }
                
            }
    });

    source_thread1.join();
    target_thread1.join();
    target_thread2.join();

    EXPECT_EQ(tuple_count, consumed_params_1.size()) << "Size of consumed tuples for Flow Target 1 does not match expected.";
    ASSERT_EQ(tuple_count, consumed_params_2.size()) << "Size of consumed tuples for Flow Target 2 does not match expected.";
    
    size_t i = 0;
    for (auto key : consumed_params_1) {
        ASSERT_EQ(i++, key) << "Consumed tuple for Flow Target 1 does not match expected.";
    }

    i = 0;
    for (auto key : consumed_params_2) {
        ASSERT_EQ(i++, key) << "Consumed tuple for Flow Target 2 does not match expected.";
    }
}

