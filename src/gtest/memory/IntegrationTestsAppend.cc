#include "IntegrationTestsAppend.h"

std::atomic<int> IntegrationTestsAppend::bar{0};    // Counter of threads, faced barrier.
std::atomic<int> IntegrationTestsAppend::passed{0}; // Number of barriers, passed by all threads.

void IntegrationTestsAppend::SetUp()
{  
  //Setup Test DFI
  rdma::Config::RDMA_MEMSIZE = 1024ul * 1024 * 4;  //4MB
  Config::DFI_FULL_SEGMENT_SIZE = (2048 + sizeof(DFI_SEGMENT_FOOTER_t));
  Config::DFI_SOURCE_SEGMENT_COUNT = 8;
  Config::DFI_REGISTRY_SERVER = rdma::Config::getIP(rdma::Config::RDMA_INTERFACE);
  rdma::Config::SEQUENCER_IP = rdma::Config::getIP(rdma::Config::RDMA_INTERFACE);
  Config::DFI_REGISTRY_PORT = 5300;
  Config::DFI_NODE_PORT = 5400;
  Config::DFI_NODES.clear();
  string dfiTestNode = rdma::Config::getIP(rdma::Config::RDMA_INTERFACE) + ":" + to_string(Config::DFI_NODE_PORT);
  Config::DFI_NODES.push_back(dfiTestNode);

  m_regServer = new RegistryServer();
  std::cout << "Start RegServer" << '\n';
  m_nodeServer = new NodeServer();
  std::cout << "Start NodeServer" << '\n';
  m_nodeClient = new NodeClient();
  std::cout << "Start NodeClient" << '\n';
  m_regClient = new RegistryClient();
  std::cout << "Start RegClient" << '\n';
}

void IntegrationTestsAppend::TearDown()
{
  if (m_nodeClient != nullptr)
  {
    delete m_nodeClient;
    m_nodeClient = nullptr;
  }
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

TEST_F(IntegrationTestsAppend, SimpleIntegrationWithAppendInts_BW)
{
  //ARRANGE
  string bufferName = "buffer1";
    
  size_t memSize = sizeof(int);

  uint32_t numberSegments = 2;
  size_t numberElements = (Config::DFI_FULL_SEGMENT_SIZE - sizeof(DFI_SEGMENT_FOOTER_t)) / memSize * numberSegments;
  // uint32_t segmentsPerWriter = 2;
  BufferHandle *buffHandle = new BufferHandle(bufferName, 1,  3, 1, Config::DFI_FULL_SEGMENT_SIZE - sizeof(DFI_SEGMENT_FOOTER_t)); //Create 1 less segment in ring to test BufferWriter creating a segment on the ring
  DFI_DEBUG("Created BufferHandle\n");
  m_regClient->registerBuffer(buffHandle);
  DFI_DEBUG("Registered Buffer in Registry\n");
  // buffHandle = m_regClient->joinBuffer(bufferName);
  // DFI_DEBUG("Created segment ring on buffer\n");

  size_t remoteOffset = m_regClient->retrieveBuffer(bufferName)->entrySegments[0].offset;

  int *rdma_buffer = (int *)m_nodeServer->getBuffer(remoteOffset);
  // for(size_t i = 0; i < numberSegments*Config::DFI_FULL_SEGMENT_SIZE/memSize; i++)
  // {
  //   std::cout << rdma_buffer[i] << " ";
  // }

  BufferWriter buffWriter(bufferName, *m_regClient, Config::DFI_SOURCE_SEGMENT_COUNT);

  //ACT
  for (size_t i = 0; i < numberElements; i++)
  {
    ASSERT_TRUE(buffWriter.add(&i, memSize));
  }

  ASSERT_TRUE(buffWriter.close());
  
  // for(size_t i = 0; i < numberSegments*Config::DFI_FULL_SEGMENT_SIZE/memSize; i++)
  // {
  //   std::cout << rdma_buffer[i] << " ";
  // }
  
  //ASSERT
  for (uint32_t j = 0; j < numberSegments; j++)
  {
    //Assert header
    DFI_SEGMENT_FOOTER_t *header = (DFI_SEGMENT_FOOTER_t *) &(rdma_buffer[(j*Config::DFI_FULL_SEGMENT_SIZE + Config::DFI_FULL_SEGMENT_SIZE - sizeof(DFI_SEGMENT_FOOTER_t)) / memSize]);
    ASSERT_EQ( (uint32_t)(Config::DFI_FULL_SEGMENT_SIZE- sizeof(DFI_SEGMENT_FOOTER_t)), header[0].counter) << "Expected size of segment (without header) did not match segment counter";
    ASSERT_EQ( (j == numberSegments - 1 ? true : false), header[0].isEndSegment()) << "isEndSegment did not match expected";

    //Assert data
    int expected = 0;
    for (uint32_t i = 0; i < (numberElements / numberSegments); i++)
    {
      ASSERT_EQ( expected, rdma_buffer[i]) << "Data value did not match";
      expected++;
    }
  }
}


TEST_F(IntegrationTestsAppend, FourAppendersConcurrent_BW)
{
  std::cout << "FourAppendersConcurrent_BW" << std::endl;
  //ARRANGE
  string bufferName = "test";
  int nodeId = 1;

  int64_t numClients = 4;
  size_t segPerClient = 2;
  int64_t numberElements = segPerClient * (Config::DFI_FULL_SEGMENT_SIZE - sizeof(DFI_SEGMENT_FOOTER_t)) / sizeof(int64_t); //Each client fill two segments
  std::vector<int64_t> *dataToWrite = new std::vector<int64_t>();
  
  for(int64_t i = 0; i < numberElements; i++)
  {
    dataToWrite->push_back(i);
  }  

  ASSERT_TRUE(m_regClient->registerBuffer(new BufferHandle(bufferName, nodeId, segPerClient, 4, Config::DFI_FULL_SEGMENT_SIZE - sizeof(DFI_SEGMENT_FOOTER_t))));
  // BufferHandle *buffHandle1 = m_regClient->joinBuffer(bufferName);
  // BufferHandle *buffHandle2 = m_regClient->joinBuffer(bufferName);
  // BufferHandle *buffHandle3 = m_regClient->joinBuffer(bufferName);
  // BufferHandle *buffHandle4 = m_regClient->joinBuffer(bufferName);

  int64_t *rdma_buffer = (int64_t *)m_nodeServer->getBuffer(0);

  // std::cout << "Buffer before appending" << '\n';
  // for (int i = 0; i < Config::DFI_FULL_SEGMENT_SIZE/sizeof(int64_t)*segPerClient*4; i++)
  // {
  //   std::cout << rdma_buffer[i] << ' ';
  // }

  BufferWriterClient<int64_t> *client1 = new BufferWriterClient<int64_t>(bufferName, dataToWrite);
  BufferWriterClient<int64_t> *client2 = new BufferWriterClient<int64_t>(bufferName, dataToWrite);
  BufferWriterClient<int64_t> *client3 = new BufferWriterClient<int64_t>(bufferName, dataToWrite);
  BufferWriterClient<int64_t> *client4 = new BufferWriterClient<int64_t>(bufferName, dataToWrite);

  //ACT
  client1->start();
  client2->start();
  client3->start();
  client4->start();
  client1->join();
  client2->join();
  client3->join();
  client4->join();
  
  //ASSERT

  // std::cout << "Buffer after appending, total int64_t's: " << numberElements << " * 4" << '\n';
  // for (int i = 0; i < Config::DFI_FULL_SEGMENT_SIZE/sizeof(int64_t)*segPerClient*4; i++)
  // {
  //   std::cout << rdma_buffer[i] << ' ';
  // }


  auto handle_ret = m_regClient->retrieveBuffer(bufferName);

  int64_t count = 0;
  auto bufferIterator = handle_ret->getNewIterator();

  size_t iterCounter = 0;

  BufferIterator::HasNextReturn hasNext;
  while ((hasNext = bufferIterator->has_next()) != BufferIterator::HasNextReturn::BUFFER_CLOSED)
  {
    if (hasNext != BufferIterator::HasNextReturn::TRUE)
      continue;
    size_t dataSize;
    int64_t *data = (int64_t *)bufferIterator->next(dataSize);
    int64_t start_counter = (iterCounter / numClients) * (numberElements / segPerClient);
    for (int64_t i = start_counter; i < (int64_t)((dataSize   / sizeof(int64_t)) + start_counter); i++, data++)
    {
      ASSERT_EQ(i, *data);
      count++;
    }
    iterCounter++;
    bufferIterator->free_all_prev_segments();
  }

  ASSERT_EQ(numberElements * numClients, count);
  m_nodeServer->localFree(rdma_buffer);
  
  delete bufferIterator;
}



TEST_F(IntegrationTestsAppend, SimpleAppendAndConsume_LAT)
{
  std::cout << "SimpleAppendAndConsume_LAT" << std::endl;
  //ARRANGE
  string bufferName = "buffer1";

  // size_t remoteOffset = 0;
  size_t memSize = sizeof(int);

  size_t numberSegments = 200;
  size_t numberElements = numberSegments;
  BufferHandle *buffHandle = new BufferHandle(bufferName, 1, numberSegments, 1, memSize, FlowOptimization::LAT);
  DFI_DEBUG("Created BufferHandle\n");
  m_regClient->registerBuffer(buffHandle);
  DFI_DEBUG("Registered Buffer in Registry\n");

  BufferWriter buffWriter(bufferName, *m_regClient, Config::DFI_SOURCE_SEGMENT_COUNT);

  for (size_t i = 0; i < numberElements; i++)
  {
    ASSERT_TRUE(buffWriter.add((void *)&i, memSize));
  }

  std::cout << "Finished appending. Closing..." << '\n';

  ASSERT_TRUE(buffWriter.close());


  // std::cout << "Buffer:" << '\n';
  // auto bufPtr = (int *)m_nodeServer->getBuffer();

  // for (size_t i = 0; i < numberElements + (numberSegments * sizeof(DFI_SEGMENT_HEADER_t) / memSize); i++)
  // {
  //   std::cout << bufPtr[i] << " ";
  // }

  auto handle_ret = m_regClient->retrieveBuffer(bufferName);
  // std::cout << "Iterator creation" << '\n';

  int count = 0;
  auto bufferIterator = handle_ret->getNewIterator();

  // std::cout << "Iterator enters while" << '\n';
  BufferIterator::HasNextReturn hasNext;
  while ((hasNext = bufferIterator->has_next()) != BufferIterator::HasNextReturn::BUFFER_CLOSED)
  {
    if (hasNext != BufferIterator::HasNextReturn::TRUE)
      continue;
    // std::cout << "Iterator Segment" << '\n';
    size_t dataSize;
    int *data = (int *)bufferIterator->next(dataSize);
    for (size_t i = 0; i < dataSize / memSize; i++, data++)
    {
      // std::cout << "data " << *data << " size: " << memSize << '\n';
      ASSERT_EQ(count, *data);
      count++;
    }
    bufferIterator->free_all_prev_segments();
  }
  ASSERT_EQ((int)numberElements, count);
  delete bufferIterator;
}


TEST_F(IntegrationTestsAppend, FourAppendersConcurrent_LAT)
{
  std::cout << "FourAppendersConcurrent_LAT" << std::endl;
  //ARRANGE
  string bufferName = "test";
  int nodeId = 1;

  auto numClients = 4;
  size_t segsPerRing = 50;  //# of segments in each ring
  size_t segPerClient = 200; //# of segments each client will write
  int64_t numberElements = segPerClient;
  size_t memSize = sizeof(int64_t);
  std::vector<int64_t> *dataToWrite = new std::vector<int64_t>();
  vector<size_t> countData(numberElements);

  for (int64_t i = 0; i < numberElements; i++)
  {
    dataToWrite->push_back(i);
  }

  ASSERT_TRUE(m_regClient->registerBuffer(new BufferHandle(bufferName, nodeId, segsPerRing, numClients, memSize, FlowOptimization::LAT)));

  BufferWriterClient<int64_t> *client1 = new BufferWriterClient<int64_t>(bufferName, dataToWrite, numClients, FlowOptimization::LAT);
  BufferWriterClient<int64_t> *client2 = new BufferWriterClient<int64_t>(bufferName, dataToWrite, numClients, FlowOptimization::LAT);
  BufferWriterClient<int64_t> *client3 = new BufferWriterClient<int64_t>(bufferName, dataToWrite, numClients, FlowOptimization::LAT);
  BufferWriterClient<int64_t> *client4 = new BufferWriterClient<int64_t>(bufferName, dataToWrite, numClients, FlowOptimization::LAT);

  //ACT & ASSERT
  auto nodeServer = m_nodeServer;
  auto regClient = m_regClient;
  size_t segmentsConsumed = 0;
  std::thread consumer([nodeServer, &bufferName, numClients, &segmentsConsumed, numberElements, segPerClient, regClient, &countData]() {
    auto handle_ret = regClient->retrieveBuffer(bufferName);

    int64_t count = 0;
    dfi::BufferIterator *bufferIterator = handle_ret->getNewIterator();

    size_t iterCounter = 0;

    BufferIterator::HasNextReturn hasNext;
    while ((hasNext = bufferIterator->has_next()) != BufferIterator::HasNextReturn::BUFFER_CLOSED)
    {
      if (hasNext != BufferIterator::HasNextReturn::TRUE)
        continue;
      size_t dataSize;
      int64_t *data = (int64_t *)bufferIterator->next(dataSize);
      int64_t start_counter = (int64_t)(iterCounter / numClients) * (numberElements / segPerClient);
      for (int64_t i = start_counter; i < (int64_t)((dataSize   / sizeof(int64_t)) + start_counter); i++, data++)
      {
        countData[i]++;        
        count++;
      }
      iterCounter++;
      segmentsConsumed++;
      bufferIterator->free_prev_segments(1);
    }
    ASSERT_EQ( numberElements * numClients, count) << "Number of written elements did not match expected"; 
    delete bufferIterator;
  });

  client1->start();
  client2->start();
  client3->start();
  client4->start();

  client1->join();
  client2->join();
  client3->join();
  client4->join();
  consumer.join();

  //Assert each value was read numClients times by consumer
  for (int64_t i = 0; i < numberElements; i++)
  {
    ASSERT_EQ((size_t)numClients, countData[i]);
  }

  ASSERT_EQ( (size_t)segPerClient * numClients, (size_t)segmentsConsumed) << "Consumed number of segments did not match expected";

}
