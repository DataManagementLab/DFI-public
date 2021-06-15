#include "TestBufferWriterReplicate.h"
#include "../../rdma-manager/src/rdma/RDMAClient.h"

std::atomic<int> TestBufferWriterReplicate::bar{0};    // Counter of threads, faced barrier.
std::atomic<int> TestBufferWriterReplicate::passed{0}; // Number of barriers, passed by all threads.
void TestBufferWriterReplicate::SetUp()
{
  //Setup Test DFI
  rdma::Config::RDMA_MEMSIZE = 1024ul * 1024 * 4; //4MB
  Config::DFI_FULL_SEGMENT_SIZE = (2048 + sizeof(DFI_SEGMENT_FOOTER_t));
  Config::DFI_SOURCE_SEGMENT_COUNT = 16;
  Config::DFI_REGISTRY_SERVER = rdma::Config::getIP(rdma::Config::RDMA_INTERFACE);
  Config::DFI_REGISTRY_PORT = 5300;
  rdma::Config::SEQUENCER_IP = rdma::Config::getIP(rdma::Config::RDMA_INTERFACE);
  Config::DFI_NODE_PORT = 5400;
  Config::DFI_NODES.clear();
  string dfiTestNode = rdma::Config::getIP(rdma::Config::RDMA_INTERFACE) + ":" + to_string(Config::DFI_NODE_PORT);
  Config::DFI_NODES.push_back(dfiTestNode);

  m_regServer = new RegistryServer();

  m_nodeServer = new NodeServer();

  m_regClient = new RegistryClient();
};

void TestBufferWriterReplicate::TearDown()
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
};

TEST_F(TestBufferWriterReplicate, testBuffer)
{
  //ARRANGE
  size_t numberElements = 5000;
  //ACT
  int *rdma_buffer = (int *)m_nodeServer->getBuffer(0);
  //ASSERT
  for (uint32_t i = 0; i < numberElements; i++)
  {
    ASSERT_TRUE(rdma_buffer[i] == 0);
  }
};


TEST_F(TestBufferWriterReplicate, testAppend_SimpleData)
{
  //ARRANGE
  string bufferName = "test";

  // size_t memSize = sizeof(int);

  uint32_t numberSegments = 2;
  size_t numberElements = (Config::DFI_FULL_SEGMENT_SIZE - sizeof(DFI_SEGMENT_FOOTER_t)) / sizeof(int) * numberSegments;
  m_regClient->registerBuffer(new BufferHandle(bufferName, 1, numberSegments, 1, Config::DFI_FULL_SEGMENT_SIZE - sizeof(DFI_SEGMENT_FOOTER_t)));
  size_t remoteOffset = m_regClient->retrieveBuffer(bufferName)->entrySegments[0].offset;

  BufferWriterReplicate buffWriter({bufferName}, *m_regClient, Config::DFI_SOURCE_SEGMENT_COUNT);


  //ACT

  for (int i = 0; i < (int)numberElements; i++)
  {
    // std::cout << i << " - ";
    ASSERT_TRUE(buffWriter.add(&i, sizeof(int)));
  }
  

  ASSERT_TRUE(buffWriter.close());
  int *rdma_buffer = (int *)m_nodeServer->getBuffer(remoteOffset);

  //ASSERT
  int expected = 0;
  for (uint32_t j = 0; j < numberSegments; j++)
  {
    for (uint32_t i = 0; i < (numberElements / numberSegments); i++)
    {
      ASSERT_EQ(expected, rdma_buffer[i + j * (Config::DFI_FULL_SEGMENT_SIZE/sizeof(int))]);
      expected++;
    }
  }

};


TEST_F(TestBufferWriterReplicate, testAppend_SingleInts)
{
  //ARRANGE
  string bufferName = "test";

  uint32_t numberSegments = 2;
  size_t numberElements = (Config::DFI_FULL_SEGMENT_SIZE - sizeof(DFI_SEGMENT_FOOTER_t)) / sizeof(int) * numberSegments;
  uint32_t expectedCounter = (Config::DFI_FULL_SEGMENT_SIZE - sizeof(DFI_SEGMENT_FOOTER_t));
  m_regClient->registerBuffer(new BufferHandle(bufferName, 1, numberSegments, 1, Config::DFI_FULL_SEGMENT_SIZE - sizeof(DFI_SEGMENT_FOOTER_t)));
  size_t remoteOffset = m_regClient->retrieveBuffer(bufferName)->entrySegments[0].offset;
  // auto buffHandle = m_regClient->sourceJoinBuffer(bufferName);
  BufferWriterReplicate buffWriter({bufferName}, *m_regClient, Config::DFI_SOURCE_SEGMENT_COUNT);

  //ACT
  for (uint32_t i = 0; i < numberElements; i++)
  {
    ASSERT_TRUE(buffWriter.add(&i, sizeof(int)));
  }

  //ASSERT
  ASSERT_TRUE(buffWriter.close());

  DFI_SEGMENT_FOOTER_t *footer = readSegmentFooter(remoteOffset);
  int *rdma_buffer = (int *)m_nodeServer->getBuffer(remoteOffset);

  // std::cout << "Buffer " << '\n';
  // for (int i = 0; i < Config::DFI_FULL_SEGMENT_SIZE/sizeof(int); i++)
  // {
  //   std::cout << rdma_buffer[i] << ' ';
  //   // result.push_back(rdma_buffer[i]);
  // }
  ASSERT_EQ(expectedCounter, footer[0].counter);
  ASSERT_TRUE(!footer[0].isEndSegment());

  for (uint32_t j = 0; j < numberSegments; j++)
  {
    int expected = 0;
    for (uint32_t i = 0; i < (numberElements / numberSegments); i++)
    {
      ASSERT_EQ(expected, rdma_buffer[i]);
      expected++;
    }
  }
};

TEST_F(TestBufferWriterReplicate, testAppend_MultipleConcurrentClients)
{
  //ARRANGE

  string bufferName = "test";
  // int nodeId = 1;
  uint32_t expectedCounter = Config::DFI_FULL_SEGMENT_SIZE - sizeof(DFI_SEGMENT_FOOTER_t);
  std::vector<int> expectedResult;
  std::vector<int> result;

  std::vector<TestData> *dataToWrite = new std::vector<TestData>();

  size_t numberElements = expectedCounter / sizeof(TestData);

  //Create data for clients to send
  for (size_t i = 0; i < numberElements; i++)
  {
    dataToWrite->emplace_back(i, i, i, i);
    for (size_t t = 0; t < 4; t++)
    {
      expectedResult.push_back(i);
    }
  }
  m_regClient->registerBuffer(new BufferHandle(bufferName, 1, 2, 2, Config::DFI_FULL_SEGMENT_SIZE - sizeof(DFI_SEGMENT_FOOTER_t)));
  auto buffHandle = m_regClient->retrieveBuffer(bufferName);
  

  BufferWriterReplicateClient<TestData> *client1 = new BufferWriterReplicateClient<TestData>(m_nodeServer, bufferName, dataToWrite);
  BufferWriterReplicateClient<TestData> *client2 = new BufferWriterReplicateClient<TestData>(m_nodeServer, bufferName, dataToWrite);

  //ACT
  client1->start();
  client2->start();
  client1->join();
  client2->join();

  //ASSERT
  int *rdma_buffer = (int *)m_nodeServer->getBuffer(0);

  // std::cout << "Buffer " << '\n';
  // for (int i = 0; i < Config::DFI_FULL_SEGMENT_SIZE/sizeof(int)*2; i++)
  // {
  //   std::cout << rdma_buffer[i] << ' ';
  //   // result.push_back(rdma_buffer[i]);
  // }

  //Assert Both Clients

  std::cout << "buffHandle->entrySegments.size() " << buffHandle->entrySegments.size()<< '\n';
  for (size_t i = 0; i < buffHandle->entrySegments.size(); i++)
  {
    DFI_SEGMENT_FOOTER_t *footer = readSegmentFooter(buffHandle->entrySegments[i].offset);
    if (footer[0].counter == (uint64_t)0)
      continue;

    ASSERT_EQ(expectedCounter, footer[0].counter);

    for (size_t j = buffHandle->entrySegments[i].offset / sizeof(int); j < (buffHandle->entrySegments[i].offset + expectedCounter) / sizeof(int); j++)
    {
      result.push_back(rdma_buffer[j]);
    }
  }
  ASSERT_EQ(expectedResult.size() * 2, result.size());
  for (size_t i = 0; i < expectedResult.size() * 2; i++)
  {
    ASSERT_EQ(expectedResult[i % expectedResult.size() ], result[i]);
  }

  result.clear();
}

void *TestBufferWriterReplicate::readSegmentData(BufferSegment *segment, size_t &size)
{
  return readSegmentData(segment->offset, size);
}

void *TestBufferWriterReplicate::readSegmentData(size_t offset, size_t &size)
{
  void *segmentPtr = m_nodeServer->getBuffer(offset);
  DFI_SEGMENT_FOOTER_t *footer = readSegmentFooter(offset);
  size = footer->counter;
  return segmentPtr;
}

DFI_SEGMENT_FOOTER_t *TestBufferWriterReplicate::readSegmentFooter(BufferSegment *segment)
{
  return readSegmentFooter(segment->offset);
}

DFI_SEGMENT_FOOTER_t *TestBufferWriterReplicate::readSegmentFooter(size_t segment_offset)
{
  void *segmentPtr = m_nodeServer->getBuffer(segment_offset);
  DFI_SEGMENT_FOOTER_t *footer = (DFI_SEGMENT_FOOTER_t *)((char*)segmentPtr + Config::DFI_FULL_SEGMENT_SIZE - sizeof(DFI_SEGMENT_FOOTER_t));
  return footer;
}
