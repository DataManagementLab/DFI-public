#include "TestRegistryClient.h"

void TestRegistryClient::SetUp()
{
  rdma::Config::RDMA_MEMSIZE = 1024ul * 1024 * 1; //1MB
  Config::DFI_FULL_SEGMENT_SIZE = (2048 + sizeof(DFI_SEGMENT_FOOTER_t));
  Config::DFI_SOURCE_SEGMENT_COUNT = 8;
  Config::DFI_REGISTRY_SERVER = rdma::Config::getIP(rdma::Config::RDMA_INTERFACE);
  rdma::Config::SEQUENCER_IP = rdma::Config::getIP(rdma::Config::RDMA_INTERFACE);
  Config::DFI_NODES.clear();
  string dfiTestNode = rdma::Config::getIP(rdma::Config::RDMA_INTERFACE) + ":" + to_string(Config::DFI_NODE_PORT);
  Config::DFI_NODES.push_back(dfiTestNode);
  m_regServer = new RegistryServer();
  std::cout << "Start RegServer" << '\n';
  m_nodeServer = new NodeServer();
  std::cout << "Start NodeServer" << '\n';

  m_regClient = new RegistryClient();
  std::cout << "Start RegClient" << '\n';
}

void TestRegistryClient::TearDown()
{
  if (m_regServer != nullptr)
  {
    m_regServer->stopServer();
    delete m_regServer;
    m_regServer = nullptr;
  }
  if (m_nodeServer != nullptr)
  {
    m_nodeServer->stopServer();
    delete m_nodeServer;
    m_nodeServer = nullptr;
  }
  if (m_regClient != nullptr)
  {
    delete m_regClient;
    m_regClient = nullptr;
  }
}

TEST_F(TestRegistryClient, testRetrieveBuffer)
{
  string name = "buffer2";
  ASSERT_TRUE(m_regClient->registerBuffer(new BufferHandle(name, 1, 1, 2, 1024)));

  auto buffHandle = m_regClient->retrieveBuffer(name);
  ASSERT_TRUE(buffHandle != nullptr);
  ASSERT_EQ(buffHandle->name, name);

  ASSERT_EQ(2, ((int)buffHandle->entrySegments.size()));
};

TEST_F(TestRegistryClient, testRegisterBuffer)
{
  string name = "buffer1";
  BufferHandle *buffHandle = new BufferHandle(name, 1, 1, 2, Config::DFI_FULL_SEGMENT_SIZE - sizeof(DFI_SEGMENT_FOOTER_t));
  ASSERT_TRUE(m_regClient->registerBuffer(buffHandle));

  std::unique_ptr<BufferHandle> handle_ret = m_regClient->retrieveBuffer(name);
  ASSERT_TRUE(handle_ret != nullptr);
  ASSERT_EQ(handle_ret->name, name);
  ASSERT_EQ(2, ((int)handle_ret->entrySegments.size()));
};

TEST_F(TestRegistryClient, testJoinBuffer)
{
  string name = "buffer1";
  BufferHandle *buffHandle = new BufferHandle(name, 1, 1, 2, Config::DFI_FULL_SEGMENT_SIZE - sizeof(DFI_SEGMENT_FOOTER_t));
  ASSERT_TRUE(m_regClient->registerBuffer(buffHandle));
  {
    auto handle_ret = m_regClient->sourceJoinBuffer(name);
    ASSERT_TRUE(handle_ret != nullptr);
    ASSERT_EQ(handle_ret->name, name);
    ASSERT_EQ(1, ((int)handle_ret->entrySegments.size()));
    ASSERT_EQ(1, ((int)handle_ret->node_id));
  }
  {
    auto handle_ret = m_regClient->sourceJoinBuffer(name);
    ASSERT_TRUE(handle_ret != nullptr);
    ASSERT_EQ(handle_ret->name, name);
    ASSERT_EQ(1, ((int)handle_ret->entrySegments.size()));
    ASSERT_EQ(1, ((int)handle_ret->node_id));
  }

};
