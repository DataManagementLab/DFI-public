#include "TestBufferWriter.h"
#include "../../rdma-manager/src/rdma/RDMAClient.h"

std::atomic<int> TestBufferWriter::bar{0};    // Counter of threads, faced barrier.
std::atomic<int> TestBufferWriter::passed{0}; // Number of barriers, passed by all threads.
void TestBufferWriter::SetUp()
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

void TestBufferWriter::TearDown()
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

TEST_F(TestBufferWriter, testBuffer)
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

TEST_F(TestBufferWriter, test_add_flush_SimpleData)
{
  //ARRANGE
  string bufferName = "test";

  // size_t remoteOffset = 0;
  const size_t memSize = 64;
  uint32_t numberSegments = 2;
  size_t numberElements = (Config::DFI_FULL_SEGMENT_SIZE - sizeof(DFI_SEGMENT_FOOTER_t)) / memSize * numberSegments;
  m_regClient->registerBuffer(new BufferHandle(bufferName, 1, numberSegments, 1, Config::DFI_FULL_SEGMENT_SIZE - sizeof(DFI_SEGMENT_FOOTER_t)));

  BufferWriter buffWriter(bufferName, *m_regClient, Config::DFI_SOURCE_SEGMENT_COUNT, nullptr, true);


  //ACT
  for (size_t i = 0; i < numberElements; i++)
  {
    uint64_t cacheline[8] __attribute__((aligned(memSize))) = {i,i,i,i,i,i,i,i};
    
    // std::cout << "Adding " << i << '\n';
    ASSERT_TRUE(buffWriter.add_nontemp(cacheline, 64));

  }

  ASSERT_TRUE(buffWriter.close());
  

    // size_t *rdma_buffer = (size_t *)m_nodeServer->getBuffer(0);

    // for (size_t i = 0; i < numberElements*8 + numberSegments*(sizeof(DFI_SEGMENT_FOOTER_t)/sizeof(uint64_t)); i++)
    //     std::cout << rdma_buffer[i] << ' ';

  BufferReader br(m_regClient->retrieveBuffer(bufferName), m_regClient);
  size_t read_data_size = 0;
  size_t *read_data = (size_t*)br.read(read_data_size);

  //ASSERT
  size_t expected = 0;
  for (uint32_t i = 0; i < numberElements; i++)
  {
      ASSERT_EQ(expected, read_data[i * 8]);
      expected++;
  }

  
}


TEST_F(TestBufferWriter, testAdd_SimpleData)
{
  //ARRANGE
  string bufferName = "test";

  // size_t memSize = sizeof(int);

  uint32_t numberSegments = 2;
  size_t numberElements = (Config::DFI_FULL_SEGMENT_SIZE - sizeof(DFI_SEGMENT_FOOTER_t)) / sizeof(int) * numberSegments;
  m_regClient->registerBuffer(new BufferHandle(bufferName, 1, numberSegments, 1, Config::DFI_FULL_SEGMENT_SIZE - sizeof(DFI_SEGMENT_FOOTER_t)));

  size_t remoteOffset = m_regClient->retrieveBuffer(bufferName)->entrySegments[0].offset;

  BufferWriter buffWriter(bufferName, *m_regClient, Config::DFI_SOURCE_SEGMENT_COUNT);


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

  std::cout << "Finished " << std::endl;
};


TEST_F(TestBufferWriter, testAdd_SingleInts)
{
  //ARRANGE
  string bufferName = "test";

  uint32_t numberSegments = 2;
  size_t numberElements = (Config::DFI_FULL_SEGMENT_SIZE - sizeof(DFI_SEGMENT_FOOTER_t)) / sizeof(int) * numberSegments;
  uint32_t expectedCounter = (Config::DFI_FULL_SEGMENT_SIZE - sizeof(DFI_SEGMENT_FOOTER_t));
  m_regClient->registerBuffer(new BufferHandle(bufferName, 1, numberSegments, 1, Config::DFI_FULL_SEGMENT_SIZE - sizeof(DFI_SEGMENT_FOOTER_t)));
  size_t remoteOffset = m_regClient->retrieveBuffer(bufferName)->entrySegments[0].offset;
  // auto buffHandle = m_regClient->joinBuffer(bufferName);
  BufferWriter buffWriter(bufferName, *m_regClient, Config::DFI_SOURCE_SEGMENT_COUNT);

  //ACT
  for (uint32_t i = 0; i < numberElements; i++)
  {
    ASSERT_TRUE(buffWriter.add(&i, sizeof(int)));
  }

  //ASSERT
  ASSERT_TRUE(buffWriter.close());

  DFI_SEGMENT_FOOTER_t *footer = readSegmentFooter(remoteOffset);
  int *rdma_buffer = (int *)m_nodeServer->getBuffer(remoteOffset);


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

TEST_F(TestBufferWriter, testAdd_MultipleConcurrentClients)
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
  // for (int i = 0; i < Config::DFI_FULL_SEGMENT_SIZE/sizeof(int)*2; i++)
  // {
  //   std::cout << rdma_buffer[i] << ' ';
  //   // result.push_back(rdma_buffer[i]);
  // }

  //Assert Both Clients

  // std::cout << "buffHandle->entrySegments.size() " << buffHandle->entrySegments.size()<< '\n';
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


TEST_F(TestBufferWriter, testAdd_SimpleData_lat)
{
  std::cout << "TestBufferWriter::testAdd_SimpleData_lat" << '\n';

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


TEST_F(TestBufferWriter, testAdd_MultipleConcurrentClients_lat)
{
  std::cout << "TestBufferWriter::testAdd_MultipleConcurrentClients_lat" << '\n';
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



TEST_F(TestBufferWriter, testAdd_BigAppends_lat)
{
  std::cout << "TestBufferWriter::testAdd_BigAppends_lat" << '\n';
  //ARRANGE
  string bufferName = "test";
  const size_t numInts = 50;
  uint32_t msgSize = numInts*sizeof(int);
  Config::DFI_FULL_SEGMENT_SIZE = msgSize + sizeof(DFI_SEGMENT_FOOTER_t);

  uint32_t numberSegments = 50;
  m_regClient->registerBuffer(new BufferHandle(bufferName, 1, numberSegments, 1, Config::DFI_FULL_SEGMENT_SIZE - sizeof(DFI_SEGMENT_FOOTER_t), FlowOptimization::LAT));
  // auto buffHandle = m_regClient->joinBuffer(bufferName);
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

void *TestBufferWriter::readSegmentData(BufferSegment *segment, size_t &size)
{
  return readSegmentData(segment->offset, size);
}

void *TestBufferWriter::readSegmentData(size_t offset, size_t &size)
{
  void *segmentPtr = m_nodeServer->getBuffer(offset);
  DFI_SEGMENT_FOOTER_t *footer = readSegmentFooter(offset);
  size = footer->counter;
  return segmentPtr;
}

DFI_SEGMENT_FOOTER_t *TestBufferWriter::readSegmentFooter(BufferSegment *segment)
{
  return readSegmentFooter(segment->offset);
}

DFI_SEGMENT_FOOTER_t *TestBufferWriter::readSegmentFooter(size_t segment_offset)
{
  void *segmentPtr = m_nodeServer->getBuffer(segment_offset);
  DFI_SEGMENT_FOOTER_t *footer = (DFI_SEGMENT_FOOTER_t *)((char*)segmentPtr + Config::DFI_FULL_SEGMENT_SIZE - sizeof(DFI_SEGMENT_FOOTER_t));
  return footer;
}
