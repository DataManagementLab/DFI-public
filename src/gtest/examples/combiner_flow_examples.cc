#include "combiner_flow_examples.h"

void CombinerFlowExamples::SetUp()
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

void CombinerFlowExamples::TearDown()
{

}

TEST_F(CombinerFlowExamples, simpleBigTupleCombiner)
{
  string flow_name = "Simple Big Tuple Combiner Flow";

    const int attr_count = 128;
    // ------- MASTER -------
    std::vector<pair<string, TypeId>> schema_vec;
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