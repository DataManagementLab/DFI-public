#include "TestBufferWriterLat.h"
#include "../../rdma-manager/src/rdma/RDMAClient.h"

std::atomic<int> TestBufferWriterLat::bar{0};    // Counter of threads, faced barrier.
std::atomic<int> TestBufferWriterLat::passed{0}; // Number of barriers, passed by all threads.
void TestBufferWriterLat::SetUp()
{
  //Setup Test DFI
  rdma::Config::RDMA_MEMSIZE = 1024ul * 1024 * 4; //4MB
  Config::DFI_FULL_SEGMENT_SIZE = (2048 + sizeof(DFI_SEGMENT_FOOTER_t));
  Config::DFI_SOURCE_SEGMENT_COUNT = 64;
  Config::DFI_REGISTRY_SERVER = rdma::Config::getIP(rdma::Config::RDMA_INTERFACE);
  Config::DFI_REGISTRY_PORT = 5300;
  rdma::Config::SEQUENCER_IP = rdma::Config::getIP(rdma::Config::RDMA_INTERFACE);
  Config::DFI_NODE_PORT = 5400;
  Config::DFI_NODES.clear();
  string dfiTestNode = rdma::Config::getIP(rdma::Config::RDMA_INTERFACE) + ":" + to_string(Config::DFI_NODE_PORT);
  Config::DFI_NODES.push_back(dfiTestNode);

  m_regServer = new RegistryServer();
  std::cout << "Started RegServer" << '\n';
  m_nodeServer = new NodeServer();


  m_regClient = new RegistryClient();
};

void TestBufferWriterLat::TearDown()
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


TEST_F(TestBufferWriterLat, testAppend_SimpleData)
{
  std::cout << "TestBufferWriterLat::testAppend_SimpleData" << '\n';

  //ARRANGE
  string bufferName = "test";

  // size_t memSize = sizeof(int);
  Config::DFI_FULL_SEGMENT_SIZE = sizeof(int) + sizeof(DFI_SEGMENT_FOOTER_t);

  uint32_t numberSegments = 10;
  size_t numberElements = numberSegments;
  m_regClient->registerBuffer(new BufferHandle(bufferName, 1, numberSegments, 1, Config::DFI_FULL_SEGMENT_SIZE - sizeof(DFI_SEGMENT_FOOTER_t), FlowOptimization::LAT));

  BufferWriter buffWriter(bufferName, *m_regClient, numberSegments/2);


  //ACT
  for (size_t i = 0; i < numberElements; i++)
  {
    ASSERT_TRUE(buffWriter.add(&i, sizeof(int)));
  }

  ASSERT_TRUE(buffWriter.close());
  auto buffHandle = m_regClient->retrieveBuffer(bufferName);
  char *rdma_buffer = (char *)m_nodeServer->getBuffer(buffHandle->entrySegments[0].offset);
  // std::cout << "Buffer " << '\n';
  // for (int i = 0; i < Config::DFI_FULL_SEGMENT_SIZE*numberElements/sizeof(int); i++)
  // {
  //   std::cout << ((int*)rdma_buffer)[i] << ' ';
  //   // result.push_back(rdma_buffer[i]);
  // }
  //ASSERT
  int expected = 0;
  for (uint32_t j = 0; j < numberSegments; j++)
  {
      ASSERT_EQ( expected, *(int*)(rdma_buffer + j * Config::DFI_FULL_SEGMENT_SIZE)) << "Expected value did not match actual!";
      expected++;
  }
};


TEST_F(TestBufferWriterLat, testAppend_MultipleConcurrentClients)
{
  std::cout << "TestBufferWriterLat::testAppend_MultipleConcurrentClients" << '\n';
  //ARRANGE

  string bufferName = "test";
  // int nodeId = 1;

  Config::DFI_FULL_SEGMENT_SIZE = sizeof(TestData) + sizeof(DFI_SEGMENT_FOOTER_t);

  uint32_t expectedCounter = sizeof(TestData);
  std::vector<int> expectedResult;
  std::vector<int> result;

  std::vector<TestData> *dataToWrite = new std::vector<TestData>();

  size_t numberElements = 200;

  //Create data for clients to send
  for (size_t i = 0; i < numberElements; i++)
  {
    dataToWrite->emplace_back(i, i, i, i);
    for (size_t t = 0; t < 4; t++)
    {
      expectedResult.push_back(i);
    }
  }
  m_regClient->registerBuffer(new BufferHandle(bufferName, 1, numberElements, 2, Config::DFI_FULL_SEGMENT_SIZE - sizeof(DFI_SEGMENT_FOOTER_t), FlowOptimization::LAT));
  auto buffHandle = m_regClient->retrieveBuffer(bufferName);
  

  BufferWriterClient<TestData> *client1 = new BufferWriterClient<TestData>(m_nodeServer, bufferName, dataToWrite);
  BufferWriterClient<TestData> *client2 = new BufferWriterClient<TestData>(m_nodeServer, bufferName, dataToWrite);

  //ACT
  client1->start();
  client2->start();
  client1->join();
  client2->join();

  //ASSERT
  int *rdma_buffer = (int *)m_nodeServer->getBuffer(0);

  // std::cout << "Buffer " << '\n';
  // for (int i = 0; i < Config::DFI_FULL_SEGMENT_SIZE*numberElements/sizeof(int)*2; i++)
  // {
  //   std::cout << rdma_buffer[i] << ' ';
  //   // result.push_back(rdma_buffer[i]);
  // }

  usleep(100); //Ensure RNIC had a chance to complete the writes before reading

  //Assert Both Clients

  // std::cout << "buffHandle->entrySegments.size() " << buffHandle->entrySegments.size()<< '\n';
  for (size_t i = 0; i < buffHandle->entrySegments.size(); i++)
  {
    DFI_SEGMENT_FOOTER_t *footer = readSegmentFooter(buffHandle->entrySegments[i].offset);
    for (size_t k = 0; k < numberElements; k++)
    {
      footer = readSegmentFooter(buffHandle->entrySegments[i].offset + k * Config::DFI_FULL_SEGMENT_SIZE);
      ASSERT_EQ(expectedCounter, footer[0].counter) << "Expected counter did not match actual!";

      for (size_t j = (buffHandle->entrySegments[i].offset + k * Config::DFI_FULL_SEGMENT_SIZE) / sizeof(int); j < (buffHandle->entrySegments[i].offset + k * Config::DFI_FULL_SEGMENT_SIZE + expectedCounter) / sizeof(int); j++)
      {
        result.push_back(rdma_buffer[j]);
      }
    }
  }
  ASSERT_EQ( expectedResult.size() * 2, result.size()) << "Expected result set size did not match actual!";
  for (size_t i = 0; i < expectedResult.size() * 2; i++)
  {
    ASSERT_EQ( expectedResult[i % expectedResult.size() ], result[i]) << "Expected value did not match actual!";
  }

  result.clear();
}



TEST_F(TestBufferWriterLat, testAppend_BigAppends)
{
  std::cout << "TestBufferWriterLat::testAppend_BigAppends" << '\n';
  //ARRANGE
  string bufferName = "test";
  const size_t numInts = 50;
  uint32_t msgSize = numInts*sizeof(int);
  Config::DFI_FULL_SEGMENT_SIZE = msgSize + sizeof(DFI_SEGMENT_FOOTER_t);

  uint32_t numberSegments = 50;
  m_regClient->registerBuffer(new BufferHandle(bufferName, 1, numberSegments, 1, Config::DFI_FULL_SEGMENT_SIZE - sizeof(DFI_SEGMENT_FOOTER_t), FlowOptimization::LAT));
  // auto buffHandle = m_regClient->sourceJoinBuffer(bufferName);
  BufferWriter buffWriter(bufferName, *m_regClient, Config::DFI_SOURCE_SEGMENT_COUNT);
  auto buffHandle = m_regClient->retrieveBuffer(bufferName);

  //ACT
  int data[numInts];
  for (uint32_t i = 0; i < numberSegments; i++)
  {
    std::fill_n(data, numInts, i);
    ASSERT_TRUE(buffWriter.add(data, msgSize));
  }

  //ASSERT
  ASSERT_TRUE(buffWriter.close());

  DFI_SEGMENT_FOOTER_t *footer = readSegmentFooter(buffHandle->entrySegments[0].offset);
  int *rdma_buffer = (int *)m_nodeServer->getBuffer(buffHandle->entrySegments[0].offset);

  ASSERT_TRUE( !footer[0].isEndSegment()) << "First footer of segment was an end segment";
  ASSERT_EQ( msgSize, footer->counter) << "Counter of footer did not match expected!";

  int expected = 0;
  for (uint32_t j = 0; j < numberSegments; j++)
  {
    for (uint32_t i = 0; i < numInts; i++)
    {
      ASSERT_EQ( expected, rdma_buffer[j*((msgSize+sizeof(DFI_SEGMENT_FOOTER_t))/sizeof(int)) + i]) << "Expected value did not match actual!";
    }
    expected++;
  }
};


DFI_SEGMENT_FOOTER_t *TestBufferWriterLat::readSegmentFooter(BufferSegment *segment)
{
  return readSegmentFooter(segment->offset);
}

DFI_SEGMENT_FOOTER_t *TestBufferWriterLat::readSegmentFooter(size_t segment_offset)
{
  void *segmentPtr = m_nodeServer->getBuffer(segment_offset);
  DFI_SEGMENT_FOOTER_t *footer = (DFI_SEGMENT_FOOTER_t *)((char*)segmentPtr + Config::DFI_FULL_SEGMENT_SIZE - sizeof(DFI_SEGMENT_FOOTER_t));
  return footer;
}
