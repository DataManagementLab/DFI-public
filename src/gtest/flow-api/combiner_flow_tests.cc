#include "combiner_flow_tests.h"

void TestCombinerFlow::SetUp()
{
    //Setup Test DFI
    rdma::Config::RDMA_MEMSIZE = 1024ul * 1024 * 8; //128MB
    Config::DFI_FULL_SEGMENT_SIZE = (2048 + sizeof(DFI_SEGMENT_FOOTER_t));
    Config::DFI_SEGMENTS_PER_RING = 12;
    Config::DFI_SOURCE_SEGMENT_COUNT = 8;
    Config::DFI_REGISTRY_SERVER = rdma::Config::getIP(rdma::Config::RDMA_INTERFACE);
    Config::DFI_REGISTRY_PORT = 5300;
    rdma::Config::SEQUENCER_IP = rdma::Config::getIP(rdma::Config::RDMA_INTERFACE);

    uint16_t serverPort = 5401;
    Config::DFI_NODES = {rdma::Config::getIP(rdma::Config::RDMA_INTERFACE) + ":" + to_string(serverPort)};

    m_regServer = std::make_unique<RegistryServer>();
    m_nodeServer = std::make_unique<DFI_Node>(serverPort);
}

void TestCombinerFlow::TearDown()
{

}

TEST_F(TestCombinerFlow, simpleCombiner_Mean_BW)
{
    std::cout << "simpleCombiner_Mean_BW" << std::endl;
    string flow_name = "Simple Combiner Flow";

    const int attr_count = 128;
    // ------- MASTER -------
    vector<pair<string, TypeId>> schema_vec;
    for (int i = 0; i < attr_count; i++)
    {
        schema_vec.push_back({"attr" + to_string(i), dfi::TypeId::DOUBLE});
    }
    // ------- Flow Source 1 -------
    double data1[attr_count];
    double data2[attr_count];
    for (uint64_t i = 0; i < attr_count; i++)
    {
        data1[i] = i;
        data2[i] = i + attr_count;
    }

    // ------- MASTER -------
    size_t sources_count = 2;
    DFI_Combiner_flow_init(flow_name, sources_count, {{1, 1}}, schema_vec, AggrFunc::MEAN, DFI_Combiner_flow_optimization::Bandwidth);

    // ------- Flow Source 1 -------
    std::thread source_thread1([flow_name, &data1]() {
        std::cout << "Creating Flow Source 1" << '\n';
        DFI_Combiner_flow_source *source = new DFI_Combiner_flow_source(flow_name, 8);
        source->push(data1);
        delete source;
    });

    // ------- Flow Source 2 -------
    std::thread source_thread2([flow_name, &data2]() {
        std::cout << "Creating Flow Source 2" << '\n';
        DFI_Combiner_flow_source *source = new DFI_Combiner_flow_source(flow_name, 8);
        source->push(data2);
        delete source;
    });

    // ------- Flow Target 1 -------
    std::thread target_thread([flow_name, attr_count, &data1, &data2]() {
        std::cout << "Creating Flow Target 1" << '\n';
        DFI_Combiner_flow_target target(flow_name, 1);

        dfi::Tuple tuple = target.create_tuple();
        size_t tuplesConsumed = 0;
        int msg;
        while ((msg = target.consume(tuple, 4, tuplesConsumed)) != DFI_FLOW_FINISHED)
        {
            ASSERT_EQ((size_t)2, tuplesConsumed);
            for (int i = 0; i < attr_count; i++)
            {
                ASSERT_EQ((data1[i] + data2[i]) / tuplesConsumed, *(double *)tuple.getDataPtr(i));
            }
        }
    });

    source_thread1.join();
    source_thread2.join();
    target_thread.join();
}

TEST_F(TestCombinerFlow, advancedCombiner_Mean_BW)
{
    std::cout << "advancedCombiner_Mean_BW" << std::endl;
    string flow_name = "Simple Combiner Flow";

    size_t tuples_fit_in_segment = 8;

    const int attr_count = 128;
    // ------- MASTER -------
    vector<pair<string, TypeId>> schema_vec;
    for (int i = 0; i < attr_count; i++)
    {
        schema_vec.push_back({"attr" + to_string(i), dfi::TypeId::DOUBLE});
    }

    Config::DFI_FULL_SEGMENT_SIZE = ((attr_count * sizeof(double) * tuples_fit_in_segment) + sizeof(DFI_SEGMENT_FOOTER_t)); // space for 8 tuples in each segment

    // ------- Flow Source 1 -------
    double data1[attr_count];
    double data2[attr_count];
    for (uint64_t i = 0; i < attr_count; i++)
    {
        data1[i] = i;
        data2[i] = i + attr_count;
    }

    // ------- MASTER -------
    size_t sources_count = 2;
    DFI_Combiner_flow_init(flow_name, sources_count, {{1, 1}}, schema_vec, AggrFunc::MEAN, DFI_Combiner_flow_optimization::Bandwidth);

    // ------- Flow Source 1 -------
    std::thread source_thread1([&]() {
        std::cout << "Creating Flow Source 1" << '\n';
        DFI_Combiner_flow_source *source = new DFI_Combiner_flow_source(flow_name, 8);
        for (size_t i = 0; i < tuples_fit_in_segment * 2; i++)
        {
            source->push(data1);
        }
        delete source;
    });

    // ------- Flow Source 2 -------
    std::thread source_thread2([&]() {
        std::cout << "Creating Flow Source 2" << '\n';
        DFI_Combiner_flow_source *source = new DFI_Combiner_flow_source(flow_name, 8);
        for (size_t i = 0; i < tuples_fit_in_segment * 2; i++)
        {
            source->push(data2);
        }
        delete source;
    });

    // ------- Flow Target 1 -------
    std::thread target_thread([&]() {
        std::cout << "Creating Flow Target 1" << '\n';
        DFI_Combiner_flow_target target(flow_name, 1, ConsumeScheme::SYNC);

        dfi::Tuple tuple = target.create_tuple();
        size_t tuplesConsumed = 0;
        int msg;
        while ((msg = target.consume(tuple, tuples_fit_in_segment * 2, tuplesConsumed)) != DFI_FLOW_FINISHED)
        {
            ASSERT_EQ((size_t)tuples_fit_in_segment * 2, tuplesConsumed);
            for (int i = 0; i < attr_count; i++)
            {
                ASSERT_NEAR(((data1[i] + data2[i]) * tuples_fit_in_segment) / tuplesConsumed, *(double *)tuple.getDataPtr(i), 0.001);
            }
        }
    });

    source_thread1.join();
    source_thread2.join();
    target_thread.join();
}

TEST_F(TestCombinerFlow, advancedCombiner_Mean_Lat)
{
    std::cout << "advancedCombiner_Mean_Lat" << std::endl;
    string flow_name = "Adv Combiner Flow";

    Config::DFI_SOURCE_SEGMENT_COUNT = 8;

    const int attr_count = 10000;
    // ------- MASTER -------
    vector<pair<string, TypeId>> schema_vec;
    for (int i = 0; i < attr_count; i++)
    {
        schema_vec.push_back({"attr" + to_string(i), dfi::TypeId::DOUBLE});
    }
    // ------- Flow Source 1 -------
    double data1[attr_count];
    double data2[attr_count];
    for (uint64_t i = 0; i < attr_count; i++)
    {
        data1[i] = i;
        data2[i] = i + attr_count;
    }

    // ------- MASTER -------
    size_t sources_count = 2;
    DFI_Combiner_flow_init(flow_name, sources_count, {{1, 1}}, schema_vec, AggrFunc::MEAN, DFI_Combiner_flow_optimization::Latency);

    // ------- Flow Source 1 -------
    std::cout << "Creating Flow Source 1" << '\n';
    DFI_Combiner_flow_source source1(flow_name);

    // ------- Flow Source 2 -------
    std::cout << "Creating Flow Source 2" << '\n';
    DFI_Combiner_flow_source source2(flow_name);

    // ------- Flow Target 1 -------
    std::cout << "Creating Flow Target 1" << '\n';
    DFI_Combiner_flow_target target(flow_name, 1);

    source1.push(data1);
    source2.push(data2);

    size_t tuplesConsumed = 0;
    dfi::Tuple tuple = target.create_tuple();
    int ret = target.consume(tuple, 2, tuplesConsumed);
    ASSERT_EQ(DFI_SUCCESS, ret);
    ASSERT_EQ(tuplesConsumed, (size_t)2);

    for (int i = 0; i < attr_count; i++)
    {
        ASSERT_NEAR((data1[i] + data2[i]) / tuplesConsumed, *(double *)tuple.getDataPtr(i), 0.001);
    }

    source1.push(data1);
    source2.push(data2);
    source1.push(data2);
    source2.push(data1);

    //Consume and aggregate 2 tuples
    ret = target.consume(tuple, 2, tuplesConsumed);
    ASSERT_EQ(DFI_SUCCESS, ret);
    ASSERT_EQ((size_t)2, tuplesConsumed);

    for (int i = 0; i < attr_count; i++)
    {
        ASSERT_NEAR((data1[i] + data2[i]) / tuplesConsumed, *(double *)tuple.getDataPtr(i), 0.001);
    }

    //Consume and aggregate 2 tuples
    ret = target.consume(tuple, 2, tuplesConsumed);
    ASSERT_EQ(DFI_SUCCESS, ret);
    ASSERT_EQ(tuplesConsumed, (size_t)2);

    for (int i = 0; i < attr_count; i++)
    {
        ASSERT_NEAR((data1[i] + data2[i]) / tuplesConsumed, *(double *)tuple.getDataPtr(i), 0.001);
    }
}

TEST_F(TestCombinerFlow, simpleCombiner_Sum_BW)
{
    std::cout << "simpleCombiner_Sum_BW" << std::endl;
    string flow_name = "Simple Combiner Flow";

    const int attr_count = 128;
    // ------- MASTER -------
    vector<pair<string, TypeId>> schema_vec;
    for (int i = 0; i < attr_count; i++)
    {
        schema_vec.push_back({"attr" + to_string(i), dfi::TypeId::BIGUINT});
    }
    // ------- Flow Source 1 -------
    uint64_t data1[attr_count];
    uint64_t data2[attr_count];
    for (uint64_t i = 0; i < attr_count; i++)
    {
        data1[i] = i;
        data2[i] = i + attr_count;
    }

    // ------- MASTER -------
    size_t sources_count = 2;
    DFI_Combiner_flow_init(flow_name, sources_count, {{1, 1}}, schema_vec, AggrFunc::SUM, DFI_Combiner_flow_optimization::Bandwidth);

    // ------- Flow Source 1 -------
    std::thread source_thread1([flow_name, &data1]() {
        std::cout << "Creating Flow Source 1" << '\n';
        DFI_Combiner_flow_source *source = new DFI_Combiner_flow_source(flow_name, 8);
        source->push(data1);
        delete source;
    });

    // ------- Flow Source 2 -------
    std::thread source_thread2([flow_name, &data2]() {
        std::cout << "Creating Flow Source 2" << '\n';
        DFI_Combiner_flow_source *source = new DFI_Combiner_flow_source(flow_name, 8);
        source->push(data2);
        delete source;
    });

    // ------- Flow Target 1 -------
    std::thread target_thread([flow_name, attr_count, &data1, &data2]() {
        std::cout << "Creating Flow Target 1" << '\n';
        DFI_Combiner_flow_target target(flow_name, 1);

        dfi::Tuple tuple = target.create_tuple();
        size_t tuplesConsumed = 0;
        int msg;
        while ((msg = target.consume(tuple, 4, tuplesConsumed)) != DFI_FLOW_FINISHED)
        {
            ASSERT_EQ((size_t)2, tuplesConsumed);
            for (int i = 0; i < attr_count; i++)
            {
                ASSERT_EQ(data1[i] + data2[i], *(uint64_t *)tuple.getDataPtr(i));
            }
        }
    });

    source_thread1.join();
    source_thread2.join();
    target_thread.join();
}

TEST_F(TestCombinerFlow, simpleCombiner_Mean_Lat)
{
    std::cout << "simpleCombiner_Mean_Lat" << std::endl;
    string flow_name = "Simple Combiner Flow";

    const int attr_count = 128;
    // ------- MASTER -------
    vector<pair<string, TypeId>> schema_vec;
    for (int i = 0; i < attr_count; i++)
    {
        schema_vec.push_back({"attr" + to_string(i), dfi::TypeId::FLOAT});
    }
    // ------- Flow Source 1 -------
    float data1[attr_count];
    float data2[attr_count];
    for (int i = 0; i < attr_count; i++)
    {
        data1[i] = i;
        data2[i] = i + attr_count;
    }

    // ------- MASTER -------
    size_t sources_count = 2;
    DFI_Combiner_flow_init(flow_name, sources_count, {{1, 1}}, schema_vec, AggrFunc::MEAN, DFI_Combiner_flow_optimization::Latency);

    // ------- Flow Source 1 -------
    std::thread source_thread1([flow_name, &data1]() {
        std::cout << "Creating Flow Source 1" << '\n';
        DFI_Combiner_flow_source *source = new DFI_Combiner_flow_source(flow_name, 8);
        source->push(data1);
        delete source;
    });

    // ------- Flow Source 2 -------
    std::thread source_thread2([flow_name, &data2]() {
        std::cout << "Creating Flow Source 2" << '\n';
        DFI_Combiner_flow_source *source = new DFI_Combiner_flow_source(flow_name, 8);
        source->push(data2);
        delete source;
    });

    // ------- Flow Target 1 -------
    std::thread target_thread([flow_name, attr_count, &data1, &data2]() {
        std::cout << "Creating Flow Target 1" << '\n';
        DFI_Combiner_flow_target target(flow_name, 1);

        dfi::Tuple tuple = target.create_tuple();
        size_t tuplesConsumed = 0;
        int msg;
        while ((msg = target.consume(tuple, 4, tuplesConsumed)) != DFI_FLOW_FINISHED)
        {
            ASSERT_EQ((size_t)2, tuplesConsumed);
            for (int i = 0; i < attr_count; i++)
            {
                ASSERT_NEAR((float)(data1[i] + data2[i]) / tuplesConsumed, *(float *)tuple.getDataPtr(i), 0.001);
            }
        }
    });

    source_thread1.join();
    source_thread2.join();
    target_thread.join();
}

TEST_F(TestCombinerFlow, simpleCombiner_Sum_Lat)
{
    std::cout << "simpleCombiner_Sum_Lat" << std::endl;
    string flow_name = "Simple Combiner Flow";

    const int attr_count = 128;
    // ------- MASTER -------
    vector<pair<string, TypeId>> schema_vec;
    for (int i = 0; i < attr_count; i++)
    {
        schema_vec.push_back({"attr" + to_string(i), dfi::TypeId::INT});
    }
    // ------- Flow Source 1 -------
    int data1[attr_count];
    int data2[attr_count];
    for (size_t i = 0; i < attr_count; i++)
    {
        data1[i] = i;
        data2[i] = i + attr_count;
    }

    // ------- MASTER -------
    size_t sources_count = 2;
    DFI_Combiner_flow_init(flow_name, sources_count, {{1, 1}}, schema_vec, AggrFunc::SUM, DFI_Combiner_flow_optimization::Latency);

    // ------- Flow Source 1 -------
    std::thread source_thread1([flow_name, &data1]() {
        std::cout << "Creating Flow Source 1" << '\n';
        DFI_Combiner_flow_source *source = new DFI_Combiner_flow_source(flow_name, 8);
        source->push(data1);
        delete source;
    });

    // ------- Flow Source 2 -------
    std::thread source_thread2([flow_name, &data2]() {
        std::cout << "Creating Flow Source 2" << '\n';
        DFI_Combiner_flow_source *source = new DFI_Combiner_flow_source(flow_name, 8);
        source->push(data2);
        delete source;
    });

    // ------- Flow Target 1 -------
    std::thread target_thread([flow_name, attr_count, &data1, &data2]() {
        std::cout << "Creating Flow Target 1" << '\n';
        DFI_Combiner_flow_target target(flow_name, 1);

        dfi::Tuple tuple = target.create_tuple();
        size_t tuplesConsumed = 0;
        int msg;
        while ((msg = target.consume(tuple, 4, tuplesConsumed)) != DFI_FLOW_FINISHED)
        {
            ASSERT_EQ((size_t)2, tuplesConsumed);
            for (int i = 0; i < attr_count; i++)
            {
                ASSERT_EQ(data1[i] + data2[i], *(int *)tuple.getDataPtr(i));
            }
        }
    });

    source_thread1.join();
    source_thread2.join();
    target_thread.join();
}

TEST_F(TestCombinerFlow, concurrentCombiner_Sum_Lat)
{
    std::cout << "concurrentCombiner_Sum_Lat" << std::endl;
    string flow_name = "concurrentCombiner_Sum_Lat";

    const int attr_count = 128;
    // ------- MASTER -------
    vector<pair<string, TypeId>> schema_vec;
    for (int i = 0; i < attr_count; i++)
    {
        schema_vec.push_back({"attr" + to_string(i), dfi::TypeId::INT});
    }
    // ------- Flow Source 1 -------
    int data1[attr_count];
    int data2[attr_count];
    for (int i = 0; i < attr_count; i++)
    {
        data1[i] = i;
        data2[i] = i + attr_count;
    }

    // ------- MASTER -------
    size_t sources_count = 2;
    DFI_Combiner_flow_init(flow_name, sources_count, {{1, 1}, {2, 1}}, schema_vec, AggrFunc::SUM, DFI_Combiner_flow_optimization::Latency);

    // ------- Flow Source 1 -------
    std::thread source_thread1([flow_name, &data1]() {
        std::cout << "Creating Flow Source 1" << '\n';
        DFI_Combiner_flow_source source(flow_name);
        for (size_t i = 0; i < 10; i++)
        {
            source.push(data1, (TargetID)1);
            source.push(data1, (TargetID)2);
        }
    });

    // ------- Flow Source 2 -------
    std::thread source_thread2([flow_name, &data2]() {
        std::cout << "Creating Flow Source 2" << '\n';
        DFI_Combiner_flow_source source2(flow_name);
        for (size_t i = 0; i < 10; i++)
        {
            source2.push(data2, (TargetID)1);
            source2.push(data2, (TargetID)2);
        }
    });

    // ------- Flow Target 1 -------
    std::thread target_thread1([flow_name, attr_count, &data1, &data2]() {
        std::cout << "Creating Flow Target 1" << '\n';
        DFI_Combiner_flow_target target(flow_name, 1, ConsumeScheme::SYNC);
        dfi::Tuple tuple;
        size_t tuplesConsumed = 0;
        int msg;
        size_t totalTuples = 0;
        while ((msg = target.consume(tuple, 2, tuplesConsumed)) != DFI_FLOW_FINISHED)
        {
            ASSERT_EQ((size_t)2, tuplesConsumed);
            for (int i = 0; i < attr_count; i++)
            {
                ASSERT_EQ(data1[i] + data2[i], *(int *)tuple.getDataPtr(i)) << "Target 1 - attr didn't match";
            }
            totalTuples += tuplesConsumed;
        }
        ASSERT_EQ((size_t)20, totalTuples); //Receiving 10 tuples from 2 sources
    });

    // ------- Flow Target 2 -------
    std::thread target_thread2([flow_name, attr_count, &data1, &data2]() {
        std::cout << "Creating Flow Target 2" << '\n';
        DFI_Combiner_flow_target target(flow_name, 2, ConsumeScheme::SYNC);

        dfi::Tuple tuple = target.create_tuple();
        size_t tuplesConsumed = 0;
        size_t totalTuples = 0;
        int msg;
        while ((msg = target.consume(tuple, 2, tuplesConsumed)) != DFI_FLOW_FINISHED)
        {
            ASSERT_EQ(tuplesConsumed, (size_t)2);
            for (int i = 0; i < attr_count; i++)
            {
                ASSERT_EQ(data1[i] + data2[i], *(int *)tuple.getDataPtr(i)) << "Target 2 - attr didn't match";
            }
            totalTuples += tuplesConsumed;
        }
        ASSERT_EQ((size_t)20, totalTuples); //Receiving 10 tuples from 2 sources
    });

    source_thread1.join();
    source_thread2.join();
    target_thread1.join();
    target_thread2.join();
}

TEST_F(TestCombinerFlow, testSyncScheme_Mean_Lat)
{
    std::cout << "testSyncScheme_Mean_Lat" << std::endl;
    string flow_name = "testSyncScheme_Mean_Lat";

    Config::DFI_SOURCE_SEGMENT_COUNT = 8;

    const int attr_count = 10000;
    // ------- MASTER -------
    vector<pair<string, TypeId>> schema_vec;
    for (int i = 0; i < attr_count; i++)
    {
        schema_vec.push_back({"attr" + to_string(i), dfi::TypeId::DOUBLE});
    }
    // ------- Flow Source 1 -------
    double data1[attr_count];
    double data2[attr_count];
    double data3[attr_count];
    double data4[attr_count];
    for (uint64_t i = 0; i < attr_count; i++)
    {
        data1[i] = i;
        data2[i] = i + attr_count;
        data3[i] = i * 2;
        data4[i] = i * 2 + attr_count;
    }

    // ------- MASTER -------
    size_t sources_count = 2;
    DFI_Combiner_flow_init(flow_name, sources_count, {{1, 1}}, schema_vec, AggrFunc::MEAN, DFI_Combiner_flow_optimization::Latency);

    // ------- Flow Source 1 -------
    std::cout << "Creating Flow Source 1" << '\n';
    DFI_Combiner_flow_source source1(flow_name, 0);

    // ------- Flow Source 2 -------
    std::cout << "Creating Flow Source 2" << '\n';
    DFI_Combiner_flow_source source2(flow_name);

    // ------- Flow Target 1 -------
    std::cout << "Creating Flow Target 1 (Sync)" << '\n';
    DFI_Combiner_flow_target target(flow_name, 1, ConsumeScheme::SYNC);

    //Target thread
    std::thread target_thread([&target, flow_name, attr_count, &data1, &data2, &data3, &data4]() {
        size_t tuplesConsumed = 0;
        dfi::Tuple tuple = target.create_tuple();
        int ret = target.consume(tuple, 2, tuplesConsumed);
        ASSERT_EQ(DFI_SUCCESS, ret);
        ASSERT_EQ((size_t)2, tuplesConsumed);

        for (int i = 0; i < attr_count; i++)
        {
            ASSERT_NEAR((data1[i] + data2[i]) / tuplesConsumed, *(double *)tuple.getDataPtr(i), 0.001);
        }

        ret = target.consume(tuple, 2, tuplesConsumed);
        ASSERT_EQ(DFI_SUCCESS, ret);
        ASSERT_EQ(tuplesConsumed, (size_t)2);

        for (int i = 0; i < attr_count; i++)
        {
            ASSERT_NEAR((data3[i] + data4[i]) / tuplesConsumed, *(double *)tuple.getDataPtr(i), 0.001);
        }
    });

    source1.push(data1); // <--|- first consume
    source1.push(data3); //    |      <---|
    usleep(100000);      //      |          |
    source2.push(data2); // <--|          |- next consume
    source2.push(data4); //           <---|

    target_thread.join();
}

TEST_F(TestCombinerFlow, testCombinerFlowInit)
{
    DFI_Schema schema({{"Attr1", TypeId::BIGINT}});

    //Targets parameter contain duplicate targetId
    ASSERT_EQ(
        (int)DFI_FAILURE,
        DFI_Combiner_flow_init("flow name", 1, {{1, 1}, {1, 1}}, schema, AggrFunc::MEAN, DFI_Combiner_flow_optimization::Latency));

    //Targets parameter contains nodeId not found in DFI_NODES vector
    ASSERT_EQ(
        (int)DFI_FAILURE,
        DFI_Combiner_flow_init("flow name", 1, {{2, 5}, {1, 1}}, schema, AggrFunc::MAX, DFI_Combiner_flow_optimization::Bandwidth));

    //No targets provided
    ASSERT_EQ(
        (int)DFI_FAILURE,
        DFI_Combiner_flow_init("flow name", 1, {}, schema, AggrFunc::MAX, DFI_Combiner_flow_optimization::Bandwidth));
}