#include "replicate_flow_examples.h"

void ReplicateFlowExamples::SetUp()
{  
  //Setup Test DFI
  rdma::Config::RDMA_MEMSIZE = 1024ul * 1024 * 8;  //8MB
  Config::DFI_FULL_SEGMENT_SIZE = (512 + sizeof(DFI_SEGMENT_FOOTER_t));
  Config::DFI_SOURCE_SEGMENT_COUNT = 8;
  Config::DFI_REGISTRY_SERVER = rdma::Config::getIP(rdma::Config::RDMA_INTERFACE);
  Config::DFI_REGISTRY_PORT = 5300;
  rdma::Config::SEQUENCER_IP = rdma::Config::getIP(rdma::Config::RDMA_INTERFACE);
  vector<uint16_t> serverPorts(targets);
  vector<string> dfi_nodes;
  for(size_t i = 0; i < targets; i++)
  {
    serverPorts[i] = 5400 + i;
    // cout << serverPorts[i] << endl;
    dfi_nodes.push_back(rdma::Config::getIP(rdma::Config::RDMA_INTERFACE) + ":" + to_string(serverPorts[i]));
  }
  Config::DFI_NODES = dfi_nodes;
  // serverPorts[0] = 5400;
  // serverPorts[1] = 5401;
  // Config::DFI_NODES = {rdma::Config::getIP(rdma::Config::RDMA_INTERFACE) + ":" + to_string(serverPorts[0]), 
  //                      rdma::Config::getIP(rdma::Config::RDMA_INTERFACE) + ":" + to_string(serverPorts[1])};

  
  // m_nodeServer1 = new NodeServer(server1Port);
  // ASSERT_TRUE(m_nodeServer1->startServer());

  // std::cout << "Starting NodeServer 2" << '\n';
  // m_nodeServer2 = new NodeServer(server2Port);
  // ASSERT_TRUE(m_nodeServer2->startServer());

  std::cout << "Starting Registry Server" << '\n';
  m_regServer = new RegistryServer();
  ASSERT_TRUE(m_regServer->startServer());
  std::cout << "Registry Server started" << '\n';
  for (size_t i = 0; i < targets; i++)
  {
    std::cout << "Starting NodeServer " << i+1 << '\n';
    NodeServer* m_nodeServer = new NodeServer(rdma::Config::RDMA_MEMSIZE, serverPorts[i]);
    ASSERT_TRUE(m_nodeServer->startServer());
    m_nodeServers.push_back(m_nodeServer);
  }  

}

void ReplicateFlowExamples::TearDown()
{
  if (m_regServer != nullptr)
  {
    m_regServer->stopServer();
    delete m_regServer;
    m_regServer = nullptr;
  }  
  for(size_t i = 0; i < targets; i++)
  {
     if (m_nodeServers[i] != nullptr)
    {
      m_nodeServers[i]->stopServer();
      delete m_nodeServers[i];
      m_nodeServers[i] = nullptr;
    }
  }
 

  // if (m_nodeServer2 != nullptr)
  // {
  //   m_nodeServer2->stopServer();
  //   delete m_nodeServer2;
  //   m_nodeServer2 = nullptr;
  // }
}

TEST_F(ReplicateFlowExamples, simpleReplicate)
{
  string flow_name = "Simple Replicate Flow";
  
  // ------- MASTER -------
  DFI_Schema schema({{"value", dfi::TypeId::BIGINT}});
  // size_t shuffle_key_index = 0;
  std::vector<source_t> sources = {{1,0}};
  vector<target_t> targets = {{1,1}, {2,1}};
  ASSERT_TRUE(DFI_Replicate_flow_init(flow_name, sources, targets, schema) != DFI_FAILURE);

  // ------- Flow Source 1 -------
  std::thread source_thread1([flow_name]() {
    std::cout << "Creating Flow Source 1" << '\n';
    DFI_Replicate_flow_source source(flow_name, {}, 8);
    for(uint64_t i = 0; i < 100; i++)
    {
      uint64_t raw_tuple[] = {i};
      // Tuple tuhple = Tuple(raw_tuple);
      // cout << "here" << endl;
      source.push(Tuple(raw_tuple));
    }
    // source.flush();    
  });

  // ------- Flow Source 2 -------
  // std::thread source_thread2([flow_name]() {
  //   std::cout << "Creating Flow Source 2" << '\n';
  //   DFI_Gather_flow_source source(flow_name, 8, {2});
  //   for(uint64_t i = 100; i < 200; i++)
  //   {
  //     uint64_t raw_tuple[] = {i};
  //     source.push(Tuple(raw_tuple));
  //   }
  // });

  // ------- Flow Target 1 -------
  vector<uint64_t> consumed_params_1;
  std::thread target_thread1([flow_name, &consumed_params_1]() {
    std::cout << "Creating Flow Target 1" << '\n';
    DFI_Replicate_flow_target target(flow_name, 1);
    dfi::Tuple tuple = target.create_tuple();
    while(target.consume(tuple) != DFI_FLOW_FINISHED)
    {
      // cout << target.consume(tuple) << endl;
      // cout << ++i << endl;
      // cout << *(uint64_t*)tuple.getDataPtr("value") << endl;
      consumed_params_1.push_back(*(uint64_t*)tuple.getDataPtr("value"));
    }
  });
  // ------- Flow Target 2 -------
  vector<uint64_t> consumed_params_2;
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
  // source_thread2.join();
  target_thread1.join();
  target_thread2.join();

  // std::cout << "Flow Target 1 consumed keys:" << '\n';
  // for(auto key : consumed_params_1)
  //   std::cout << key << " - ";
  // cout << endl;
  // std::cout << "Flow Target 2 consumed keys:" << '\n';
  // for(auto key : consumed_params_2)
  //   std::cout << key << " - ";
  


  // ASSERT_EQ( (size_t)100, consumed_params.size()) << "Size of consumed tuples for Flow Target does not match expected.";
  // ASSERT_EQ( (size_t)50, consumed_keys_2.size()) << "Size of consumed tuples for Flow Target 2 does not match expected.";
}



// TEST_F(ShuffleFlowExamples, paperExample)
// {
//   string buffer_name = "buffer";
//   char data1[] = "Hello ";
//   char data2[] = "World!";
//   int rcv_node_id = 1; //ID is mapped to a concrete node in cluster spec
//   DFI_Context context;
//   DFI_Init(context); 
//   DFI_Create_buffer(buffer_name, rcv_node_id, context);
//   DFI_Append(buffer_name, (void*)data1, sizeof(data1), context);
//   DFI_Append(buffer_name, (void*)data2, sizeof(data2), context);
//   DFI_Close_buffer(buffer_name, context);

//   size_t buffer_size = 0;
//   void* buffer_ptr = nullptr;
//   DFI_Get_buffer(buffer_name, buffer_size, buffer_ptr, context);

//   for(size_t i = 0; i < buffer_size; i++)
//   {
//     cout << ((char*)buffer_ptr)[i];
//   }
//   DFI_Finalize(context);
// }