#include "shuffle_flow_examples.h"

void ShuffleFlowExamples::SetUp()
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
  dfi::Config::DFI_NODES = {rdma::Config::getIP(rdma::Config::RDMA_INTERFACE) + ":" + to_string(server1Port), 
                       rdma::Config::getIP(rdma::Config::RDMA_INTERFACE) + ":" + to_string(server2Port)};

}

void ShuffleFlowExamples::TearDown()
{

}

TEST_F(ShuffleFlowExamples, simpleShuffle)
{
  //Registry and node setup
  RegistryServer registry;
  DFI_Node node1(5400);
  DFI_Node node2(5401);

  //Flow initialization
  std::string flow_name = "Shuffle-example";
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
  source.push(&data);
  data = {8,20};
  source.push(&data);
  source.close();

  dfi::Tuple tuple1;
  target1.consume(tuple1);

  dfi::Tuple tuple2;
  target2.consume(tuple2);

  std::cout << "Target 1 got tuple (" << tuple1.getAs<int>("key") << "," << tuple1.getAs<int>("value") << ")" << std::endl;
  std::cout << "Target 2 got tuple (" << tuple2.getAs<int>("key") << "," << tuple2.getAs<int>("value") << ")" << std::endl;
}
