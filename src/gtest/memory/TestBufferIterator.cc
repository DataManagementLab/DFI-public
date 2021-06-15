#include "TestBufferIterator.h"
#include <chrono>

std::atomic<int> TestBufferIterator::bar{0};    // Counter of threads, faced barrier.
std::atomic<int> TestBufferIterator::passed{0}; // Number of barriers, passed by all threads.

void TestBufferIterator::SetUp()
{
  //Setup Test DFI
  rdma::Config::RDMA_MEMSIZE = 1024ul * 1024 * 4; //4MB
  Config::DFI_FULL_SEGMENT_SIZE = (512 + sizeof(DFI_SEGMENT_FOOTER_t));
  Config::DFI_SOURCE_SEGMENT_COUNT = 8;

  //BENCHMARK STUFF!!!
  // Config::RDMA_MEMSIZE = 1024ul * 1024 * 1024 * 10;  //1GB
  // Config::DFI_FULL_SEGMENT_SIZE = (80 * 1048576 + sizeof(DFI_SEGMENT_HEADER_t)); //80 MiB
  // Config::DFI_INTERNAL_BUFFER_SIZE = 1024 * 1024 * 8;

  Config::DFI_REGISTRY_SERVER = rdma::Config::getIP(rdma::Config::RDMA_INTERFACE);
  Config::DFI_REGISTRY_PORT = 5300;
  rdma::Config::SEQUENCER_IP = rdma::Config::getIP(rdma::Config::RDMA_INTERFACE);
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

void TestBufferIterator::TearDown()
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

TEST_F(TestBufferIterator, testSegmentIterator)
{

  //ARRANGE
  string bufferName = "buffer1";

  // size_t remoteOffset = 0;
  size_t memSize = sizeof(int);

  size_t numberSegments = 4;
  size_t numberElements = (Config::DFI_FULL_SEGMENT_SIZE - sizeof(DFI_SEGMENT_FOOTER_t)) / memSize * numberSegments;
  BufferHandle *buffHandle = new BufferHandle(bufferName, 1, numberSegments, 1, Config::DFI_FULL_SEGMENT_SIZE - sizeof(DFI_SEGMENT_FOOTER_t)); 
  DFI_DEBUG("Created BufferHandle\n");
  m_regClient->registerBuffer(buffHandle);
  DFI_DEBUG("Registered Buffer in Registry\n");

  BufferWriter buffWriter(bufferName, *m_regClient, Config::DFI_SOURCE_SEGMENT_COUNT);

  for (size_t i = 0; i < numberElements; i++)
  {
    ASSERT_TRUE(buffWriter.add(&i, memSize));
  }

  std::cout << "Finished appending. Closing..." << '\n';

  ASSERT_TRUE(buffWriter.close());

  auto handle_ret = m_regClient->retrieveBuffer(bufferName);
  auto endIterator = handle_ret->entrySegments[0].end();

  size_t count = 0;
  auto it = handle_ret->entrySegments[0].begin((char *)m_nodeServer->getBuffer(0), buffHandle->segmentsPerWriter);
  auto it2 = it;
  for (; it != endIterator; it++)
  {
    size_t dataSize;
    int *data = (int *)it.getRawData(dataSize);

    for (size_t i = 0; i < dataSize / memSize; i++, data++)
    {
      ASSERT_EQ((int)count, *data);
      count++;
    }
  }
  
  ASSERT_EQ(numberElements, count);
  it2->markEndSegment();
  it2->counter = 0;
  count = 0;
  for (; it2 != endIterator; it2++)
  {
    size_t dataSize;
    int *data = (int *)it2.getRawData(dataSize);

    for (size_t i = 0; i < dataSize / memSize; i++, data++)
    {
      ASSERT_EQ((int)count, *data);
      count++;
    }
  }
  ASSERT_EQ((size_t)0, count);
}

TEST_F(TestBufferIterator, testBufferIterator)
{

  //ARRANGE
  string bufferName = "buffer1";

  // size_t remoteOffset = 0;
  size_t memSize = sizeof(int);

  size_t numberSegments = 4;
  size_t numberElements = (Config::DFI_FULL_SEGMENT_SIZE - sizeof(DFI_SEGMENT_FOOTER_t)) / memSize * numberSegments;
  BufferHandle *buffHandle = new BufferHandle(bufferName, 1, numberSegments, 1, Config::DFI_FULL_SEGMENT_SIZE - sizeof(DFI_SEGMENT_FOOTER_t)); 
  DFI_DEBUG("Created BufferHandle\n");
  m_regClient->registerBuffer(buffHandle);
  DFI_DEBUG("Registered Buffer in Registry\n");

  BufferWriter buffWriter(bufferName, *m_regClient, Config::DFI_SOURCE_SEGMENT_COUNT);

  for (size_t i = 0; i < numberElements; i++)
  {
    ASSERT_TRUE(buffWriter.add(&i, memSize));
  }

  ASSERT_TRUE(buffWriter.close());

  auto handle_ret = m_regClient->retrieveBuffer(bufferName);
  // std::cout << "Iterator creation" << '\n';

  size_t count = 0;

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
      // std::cout << "data " << *data << '\n';
      ASSERT_EQ((int)count, *data);
      count++;
    }
    bufferIterator->free_all_prev_segments();
  }
  ASSERT_EQ(numberElements, count);
  
  delete bufferIterator;
}


TEST_F(TestBufferIterator, testTwoSegmentNoWrite_BW)
{
  std::cout << "testTwoSegmentNoWrite_BW" << std::endl;

  //ARRANGE
  string bufferName = "buffer1";

  size_t numberSegments = 2;
  BufferHandle *buffHandle = new BufferHandle(bufferName, 1, numberSegments, 1, Config::DFI_FULL_SEGMENT_SIZE - sizeof(DFI_SEGMENT_FOOTER_t), FlowOptimization::BW); 
  m_regClient->registerBuffer(buffHandle);

  BufferWriter buffWriter(bufferName, *m_regClient, Config::DFI_SOURCE_SEGMENT_COUNT);

  ASSERT_TRUE(buffWriter.close());

  auto handle_ret = m_regClient->retrieveBuffer(bufferName);
  // std::cout << "Iterator creation" << '\n';

  auto bufferIterator = handle_ret->getNewIterator();

  // std::cout << "Iterator enters while" << '\n';
  ASSERT_EQ( BufferIterator::HasNextReturn::BUFFER_CLOSED, bufferIterator->has_next()) << "Buffer Iterator did not return Buffer Closed, even though no segments was appended and buffer is closed";
}

TEST_F(TestBufferIterator, testTwoSegmentNoWrite_LAT)
{
  std::cout << "testTwoSegmentNoWrite_LAT" << std::endl;
  //ARRANGE
  string bufferName = "buffer1";

  // size_t remoteOffset = 0;

  size_t numberSegments = 2;
  BufferHandle *buffHandle = new BufferHandle(bufferName, 1, numberSegments, 1, Config::DFI_FULL_SEGMENT_SIZE - sizeof(DFI_SEGMENT_FOOTER_t), FlowOptimization::LAT); 
  m_regClient->registerBuffer(buffHandle);

  BufferWriter buffWriter(bufferName, *m_regClient, Config::DFI_SOURCE_SEGMENT_COUNT);

  ASSERT_TRUE(buffWriter.close());

  auto handle_ret = m_regClient->retrieveBuffer(bufferName);
  // std::cout << "Iterator creation" << '\n';

  auto bufferIterator = handle_ret->getNewIterator();

  // std::cout << "Iterator enters while" << '\n';
  ASSERT_EQ( BufferIterator::HasNextReturn::BUFFER_CLOSED, bufferIterator->has_next()) << "Buffer Iterator did not return Buffer Closed, even though no segments was appended and buffer is closed";
  
  delete bufferIterator;
}

TEST_F(TestBufferIterator, testBufferIteratorSegmentsSameSizeAsMsgs_NotInterleaved)
{

  //ARRANGE
  string bufferName = "buffer1";

  // size_t remoteOffset = 0;
  size_t memSize = sizeof(int);

  size_t numberSegments = 200;
  size_t numberElements = numberSegments;
  BufferHandle *buffHandle = new BufferHandle(bufferName, 1, numberSegments, 1, memSize); 
  DFI_DEBUG("Created BufferHandle\n");
  m_regClient->registerBuffer(buffHandle);
  DFI_DEBUG("Registered Buffer in Registry\n");

  BufferWriter buffWriter(bufferName, *m_regClient, Config::DFI_SOURCE_SEGMENT_COUNT);

  for (size_t i = 0; i < numberElements; i++)
  {
    ASSERT_TRUE(buffWriter.add(&i, memSize));
  }

  std::cout << "Finished appending. Closing..." << '\n';

  ASSERT_TRUE(buffWriter.close());

  auto handle_ret = m_regClient->retrieveBuffer(bufferName);
  // std::cout << "Iterator creation" << '\n';

  size_t count = 0;

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
      // std::cout << "data " << *data << '\n';
      ASSERT_EQ((int)count, *data);
      count++;
    }
    bufferIterator->free_all_prev_segments();
  }
  ASSERT_EQ(numberElements, count);
}


TEST_F(TestBufferIterator, testBufferIteratorFourAppenderSegSizeSameAsMsg_Interleaved)
{
  //ARRANGE
  string bufferName = "test";
  int nodeId = 1;

  auto numClients = 4;
  size_t segsPerRing = 50;  //# of segments in each ring
  size_t segPerClient = 200; //# of segments each client will write
  int64_t numberElements = segPerClient;
  std::vector<std::atomic<size_t>> countData(numberElements);
  
  size_t memSize = sizeof(int64_t);
  std::vector<int64_t> *dataToWrite = new std::vector<int64_t>();

  for (int64_t i = 0; i < numberElements; i++)
  {
    dataToWrite->push_back(i);
  }

  ASSERT_TRUE(m_regClient->registerBuffer(new BufferHandle(bufferName, nodeId, segsPerRing, numClients, memSize)));

  BufferWriterClient<int64_t> *client1 = new BufferWriterClient<int64_t>(bufferName, dataToWrite);
  BufferWriterClient<int64_t> *client2 = new BufferWriterClient<int64_t>(bufferName, dataToWrite);
  BufferWriterClient<int64_t> *client3 = new BufferWriterClient<int64_t>(bufferName, dataToWrite);
  BufferWriterClient<int64_t> *client4 = new BufferWriterClient<int64_t>(bufferName, dataToWrite);

  //ACT & ASSERT
  auto nodeServer = m_nodeServer;
  auto regClient = m_regClient;
  size_t segmentsConsumed = 0;
  std::thread consumer([nodeServer, &bufferName, numClients, &segmentsConsumed, numberElements, segPerClient, regClient, &countData]() {
    auto handle_ret = regClient->retrieveBuffer(bufferName);

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
      for (int64_t i = start_counter; i < (int64_t)((dataSize   / sizeof(int64_t)) + + start_counter); i++, data++)
      {
        countData[i]++;
        count++;
      }
      iterCounter++;
      segmentsConsumed++;
      bufferIterator->free_all_prev_segments();
    }

    ASSERT_EQ(numberElements * numClients, count); 
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

  //Assert each value was read 4 times by consumer
  for (int64_t i = 0; i < numberElements; i++)
  {
    ASSERT_EQ((size_t)4, countData[i].load());
  }

  ASSERT_EQ( (size_t)segPerClient * 4, (size_t)segmentsConsumed) << "Consumed number of segments did not match expected";

}


TEST_F(TestBufferIterator, testBufferIteratorFourAppender_NotInterleaved)
{

  //ARRANGE
  string bufferName = "test";
  int nodeId = 1;
  int numClients = 4;

  size_t segPerClient = 4;
  int64_t numberElements = (Config::DFI_FULL_SEGMENT_SIZE - sizeof(DFI_SEGMENT_FOOTER_t)) / sizeof(int64_t) * segPerClient;
  std::vector<int64_t> *dataToWrite = new std::vector<int64_t>();

  for (int64_t i = 0; i < numberElements; i++)
  {
    dataToWrite->push_back(i);
  }

  ASSERT_TRUE(m_regClient->registerBuffer(new BufferHandle(bufferName, nodeId, segPerClient, numClients, Config::DFI_FULL_SEGMENT_SIZE - sizeof(DFI_SEGMENT_FOOTER_t))));

  for (int64_t i = 0; i < numClients; i++)
  {
    BufferWriter buffWriter(bufferName, *m_regClient, Config::DFI_SOURCE_SEGMENT_COUNT);

    for (int64_t i = 0; i < numberElements; i++)
    {
      ASSERT_TRUE(buffWriter.add(&i, sizeof(int64_t)));
    }

    std::cout << "Finished appending. Closing..." << '\n';

    ASSERT_TRUE(buffWriter.close());
  }
  

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
    for (int64_t i = start_counter; i < (int64_t)((dataSize   / sizeof(int64_t)) + + start_counter); i++, data++)
    {
      ASSERT_EQ(i, *data);
      count++;
    }
    iterCounter++;
    bufferIterator->free_all_prev_segments();
  }

  ASSERT_EQ(numberElements * numClients, count);
}


TEST_F(TestBufferIterator, testBufferIteratorFourAppender_Interleaved)
{
  //ARRANGE
  string bufferName = "test";
  int nodeId = 1;

  size_t segsPerRing = 3;  //Only create 3 segments per ring
  size_t segPerClient = 6; //Each appender wants to write 6 segments
  int64_t numberElements = segPerClient * (Config::DFI_FULL_SEGMENT_SIZE - sizeof(DFI_SEGMENT_FOOTER_t)) / sizeof(int64_t);
  std::vector<int64_t> *dataToWrite = new std::vector<int64_t>();
  std::vector<std::atomic<size_t>> countData(numberElements);

  for (int64_t i = 0; i < numberElements; i++)
  {
    dataToWrite->push_back(i);
  }

  ASSERT_TRUE(m_regClient->registerBuffer(new BufferHandle(bufferName, nodeId, segsPerRing, 4, Config::DFI_FULL_SEGMENT_SIZE - sizeof(DFI_SEGMENT_FOOTER_t))));

  BufferWriterClient<int64_t> *client1 = new BufferWriterClient<int64_t>(bufferName, dataToWrite);
  BufferWriterClient<int64_t> *client2 = new BufferWriterClient<int64_t>(bufferName, dataToWrite);
  BufferWriterClient<int64_t> *client3 = new BufferWriterClient<int64_t>(bufferName, dataToWrite);
  BufferWriterClient<int64_t> *client4 = new BufferWriterClient<int64_t>(bufferName, dataToWrite);

  //ACT & ASSERT
  auto nodeServer = m_nodeServer;
  auto regClient = m_regClient;
  auto numClients = 4;
  size_t segmentsConsumed = 0;
  std::thread consumer([nodeServer, &bufferName, numClients, &segmentsConsumed, numberElements, segPerClient, regClient, &countData]() {
   
    
    auto handle_ret = regClient->retrieveBuffer(bufferName);

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
      for (int64_t i = start_counter; i < (int64_t)((dataSize   / sizeof(int64_t)) + + start_counter); i++, data++)
      {
        countData[i]++;
        count++;
      }
      iterCounter++;
      segmentsConsumed++;
      bufferIterator->free_all_prev_segments();
    }

    ASSERT_EQ(numberElements * numClients, count); 
    
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
    ASSERT_EQ((size_t)numClients, countData[i].load());
  }

  ASSERT_EQ( (size_t)segPerClient * numClients, (size_t)segmentsConsumed) << "Consumed number of segments did not match expected";

  // m_nodeServer->localFree(rdma_buffer);
}


// TEST_F(TestBufferIterator, AppendAndConsumeNotInterleaved_ReuseSegs)
// {
//   //ARRANGE
//   string bufferName = "buffer1";

//   // size_t remoteOffset = 0;
//   size_t memSize = sizeof(int);

//   size_t numberSegments = 4;
//   size_t numberElements = (Config::DFI_FULL_SEGMENT_SIZE - sizeof(DFI_SEGMENT_HEADER_t)) / memSize * numberSegments;
//   BufferHandle *buffHandle = new BufferHandle(bufferName, 1, numberSegments, 1); 
//   DFI_DEBUG("Created BufferHandle\n");
//   m_regClient->registerBuffer(buffHandle);
//   DFI_DEBUG("Registered Buffer in Registry\n");
//   // buffHandle = m_regClient->joinBuffer(bufferName);
//   // DFI_DEBUG("Created segment ring on buffer\n");

//   // int *rdma_buffer = (int *)m_nodeServer->getBuffer(remoteOffset);
//   // for(size_t i = 0; i < numberSegments*Config::DFI_FULL_SEGMENT_SIZE/memSize; i++)
//   // {
//   //   std::cout << rdma_buffer[i] << " ";
//   // } std::cout << std::endl;

//   BufferWriter buffWriter(bufferName, m_regClient, Config::DFI_INTERNAL_BUFFER_SIZE);

//   BufferConsumerBW buffConsumer(bufferName, m_regClient, m_nodeServer);

//   for (size_t i = 0; i < numberElements; i++)
//   {
//     ASSERT_TRUE(buffWriter.add(&i, memSize));
//   }

//   std::cout << "Finished appending. Closing..." << '\n';

//   ASSERT_TRUE(buffWriter.close());

//   // for(size_t i = 0; i < numberSegments*Config::DFI_FULL_SEGMENT_SIZE/memSize; i++)
//   // {
//   //   if (i % (Config::DFI_FULL_SEGMENT_SIZE/memSize) == 0)
//   //     std::cout << std::endl;
//   //   std::cout << rdma_buffer[i] << " ";
//   // } std::cout << std::endl;

//   //ACT & ASSERT
//   std::cout << "Consuming..." << '\n';
//   size_t i = 0;
//   int expected = 0;
//   size_t segmentSize = 0;
//   int *segment = (int *)buffConsumer.consume(segmentSize);
//   std::cout << "Consumed first segment" << '\n';
//   int *lastSegment;

//   while (segment != nullptr)
//   {
//     ++i;

//     ASSERT_EQ( Config::DFI_FULL_SEGMENT_SIZE - sizeof(DFI_SEGMENT_HEADER_t), segmentSize) << "Segment size did not match expected";
//     for (uint32_t j = 0; j < segmentSize / memSize; j++)
//     {
//       ASSERT_EQ( expected, segment[j]) << "Data value did not match";
//       expected++;
//     }
//     lastSegment = segment;
//     segment = (int *)buffConsumer.consume(segmentSize);

//     //Assert header on last segment
//     DFI_SEGMENT_HEADER_t *header = (DFI_SEGMENT_HEADER_t *)((char *)lastSegment - sizeof(DFI_SEGMENT_HEADER_t));
//     ASSERT_EQ( (uint64_t)0, header->counter) << "Counter did not reset";
//     ASSERT_EQ( false, Config::DFI_SEGMENT_HEADER_FLAGS::isConsumable(header->segmentFlags)) << "CanConsume did not match expected";
//     ASSERT_EQ( true, Config::DFI_SEGMENT_HEADER_FLAGS::isWriteable(header->segmentFlags)) << "CanWrite did not match expected";
//   }
//   ASSERT_EQ(numberSegments, i);

//   // for(size_t i = 0; i < numberSegments*Config::DFI_FULL_SEGMENT_SIZE/memSize; i++)
//   // {
//   //   std::cout << rdma_buffer[i] << " ";
//   // } std::cout << std::endl;
// }

// TEST_F(TestBufferIterator, FourAppendersOneConsumerInterleaved_DontReuseSegs)
// {
//   //ARRANGE
//   string bufferName = "test";
//   int nodeId = 1;

//   size_t segPerClient = 3;
//   int64_t numberElements = segPerClient * (Config::DFI_FULL_SEGMENT_SIZE - sizeof(DFI_SEGMENT_HEADER_t)) / sizeof(int64_t);
//   std::vector<int64_t> *dataToWrite = new std::vector<int64_t>();

//   for (int64_t i = 0; i < numberElements; i++)
//   {
//     dataToWrite->push_back(i);
//   }

//   ASSERT_TRUE(m_regClient->registerBuffer(new BufferHandle(bufferName, nodeId, segPerClient, 4)));

//   BufferWriterClient<int64_t> *client1 = new BufferWriterClient<int64_t>(bufferName, dataToWrite);
//   BufferWriterClient<int64_t> *client2 = new BufferWriterClient<int64_t>(bufferName, dataToWrite);
//   BufferWriterClient<int64_t> *client3 = new BufferWriterClient<int64_t>(bufferName, dataToWrite);
//   BufferWriterClient<int64_t> *client4 = new BufferWriterClient<int64_t>(bufferName, dataToWrite);

//   //ACT & ASSERT
//   auto nodeServer = m_nodeServer;
//   auto regClient = m_regClient;
//   bool *runConsumer = new bool(true);
//   size_t *segmentsConsumed = new size_t(0);
//   std::thread consumer([nodeServer, &bufferName, &runConsumer, segmentsConsumed, regClient]() {
//     BufferConsumerBW buffConsumer(bufferName, regClient, nodeServer);
//     size_t segmentSize = 0;
//     int *lastSegment;
//     while (*runConsumer)
//     {
//       int *segment = (int *)buffConsumer.consume(segmentSize);
//       while (segment != nullptr)
//       {
//         ++(*segmentsConsumed);
//         // std::cout << "Offset: " << (char*)segment - (char*)rdma_buffer << '\n';
//         if (Config::DFI_FULL_SEGMENT_SIZE - sizeof(DFI_SEGMENT_HEADER_t) != segmentSize)
//         {
//           std::cout << "SEGMENT SIZE DID NOT MATCH COUNTER!!! counter: " << segmentSize << '\n';
//           // int64_t *rdma_buffer = (int64_t *)segment;
//           // for (uint32_t j = 0; j < segmentSize/sizeof(int64_t); j++)
//           // {
//           //   std::cout << rdma_buffer[j] << ' ';
//           // }
//           break;
//         }
//         ASSERT_EQ( Config::DFI_FULL_SEGMENT_SIZE - sizeof(DFI_SEGMENT_HEADER_t), segmentSize) << "Segment size did not match expected";
//         // for (uint32_t j = 0; j < segmentSize/sizeof(int64_t); j++)
//         // {
//         //   ASSERT_EQ( expected, segment[j]) << "Data value did not match";
//         //   expected++;
//         // }
//         lastSegment = segment;
//         segment = (int *)buffConsumer.consume(segmentSize);

//         //Assert header on last segment
//         DFI_SEGMENT_HEADER_t *header = (DFI_SEGMENT_HEADER_t *)((char *)lastSegment - sizeof(DFI_SEGMENT_HEADER_t));
//         auto offsetStr = to_string((char *)lastSegment - (char *)nodeServer->getBuffer());
//         CPPUNIT_ASSERT_EQUAL_MESSAGE("Counter did not reset, on offset: " + offsetStr, (uint64_t)0, header->counter);
//         CPPUNIT_ASSERT_EQUAL_MESSAGE("CanConsume did not match expected on offset: " + offsetStr, false, Config::DFI_SEGMENT_HEADER_FLAGS::isConsumable(header->segmentFlags));
//         CPPUNIT_ASSERT_EQUAL_MESSAGE("CanWrite did not match expected on offset: " + offsetStr, true, Config::DFI_SEGMENT_HEADER_FLAGS::isWriteable(header->segmentFlags));
//       }
//       usleep(100);
//     }
//   });

//   client1->start();
//   client2->start();
//   client3->start();
//   client4->start();

//   client1->join();
//   client2->join();
//   client3->join();
//   client4->join();
//   *runConsumer = false;
//   consumer.join();

//   // std::cout << "Buffer after appending, total int64_t's: " << numberElements << " * 4" << '\n';
//   // for (size_t i = 0; i < Config::DFI_FULL_SEGMENT_SIZE/sizeof(int64_t)*segPerClient*4; i++)
//   // {
//   //   if (i % (Config::DFI_FULL_SEGMENT_SIZE/sizeof(int64_t)) == 0)
//   //     std::cout << std::endl;
//   //   std::cout << rdma_buffer[i] << ' ';
//   // }
//   ASSERT_EQ((size_t)segPerClient * 4, (size_t)*segmentsConsumed);

//   // m_nodeServer->localFree(rdma_buffer);
// }


// TEST_F(TestBufferIterator, AppenderConsumerBenchmark)
// {
//   //ARRANGE

//   string bufferName = "test";
//   int nodeId = 1;

//   size_t segsPerRing = 20;   //Only create segsPerRing segments per ring
//   size_t segPerClient = 100; //Each appender wants to write segPerClient segments

//   size_t dataSize = 1024 * 8;

//   void *scratchPad = malloc(dataSize);
//   memset(scratchPad, 1, dataSize);

//   ASSERT_TRUE(m_regClient->registerBuffer(new BufferHandle(bufferName, nodeId, segsPerRing, 1)));

//   // BufferHandle *buffHandle = m_regClient->joinBuffer(bufferName);
//   BufferWriter buffWriter(bufferName, new RegistryClient(), Config::DFI_INTERNAL_BUFFER_SIZE);

//   //ACT & ASSERT

//   std::cout << "Starting consumer" << '\n';

//   auto nodeServer = m_nodeServer;
//   auto regClient = m_regClient;
//   bool *runConsumer = new bool(true);
//   size_t *segmentsConsumed = new size_t(0);
//   std::thread consumer([nodeServer, &bufferName, &runConsumer, segmentsConsumed, regClient]() {
//     BufferConsumerBW buffConsumer(bufferName, regClient, nodeServer);
//     size_t segmentSize = 0;
//     int *lastSegment;
//     while (*runConsumer)
//     {
//       int *segment = (int *)buffConsumer.consume(segmentSize);
//       while (segment != nullptr)
//       {
//         ++(*segmentsConsumed);
//         // std::cout << "Offset: " << (char*)segment - (char*)rdma_buffer << '\n';
//         if (Config::DFI_FULL_SEGMENT_SIZE - sizeof(DFI_SEGMENT_HEADER_t) != segmentSize)
//         {
//           std::cout << "SEGMENT SIZE DID NOT MATCH COUNTER!!! counter: " << segmentSize << '\n';
//           // int64_t *rdma_buffer = (int64_t *)segment;
//           // for (uint32_t j = 0; j < segmentSize/sizeof(int64_t); j++)
//           // {
//           //   std::cout << rdma_buffer[j] << ' ';
//           // }
//           break;
//         }
//         // ASSERT_EQ( Config::DFI_FULL_SEGMENT_SIZE - sizeof(DFI_SEGMENT_HEADER_t), segmentSize) << "Segment size did not match expected";
//         // for (uint32_t j = 0; j < segmentSize/sizeof(int64_t); j++)
//         // {
//         //   ASSERT_EQ( expected, segment[j]) << "Data value did not match";
//         //   expected++;
//         // }
//         lastSegment = segment;
//         segment = (int *)buffConsumer.consume(segmentSize);

//         //Assert header on last segment
//         DFI_SEGMENT_HEADER_t *header = (DFI_SEGMENT_HEADER_t *)((char *)lastSegment - sizeof(DFI_SEGMENT_HEADER_t));
//         auto offsetStr = to_string((char *)lastSegment - (char *)nodeServer->getBuffer());
//         CPPUNIT_ASSERT_EQUAL_MESSAGE("Counter did not reset, on offset: " + offsetStr, (uint64_t)0, header->counter);
//         CPPUNIT_ASSERT_EQUAL_MESSAGE("CanConsume did not match expected on offset: " + offsetStr, false, Config::DFI_SEGMENT_HEADER_FLAGS::isConsumable(header->segmentFlags));
//         CPPUNIT_ASSERT_EQUAL_MESSAGE("CanWrite did not match expected on offset: " + offsetStr, true, Config::DFI_SEGMENT_HEADER_FLAGS::isWriteable(header->segmentFlags));
//         // usleep(100000);
//       }
//     }
//   });

//   auto startTime = chrono::high_resolution_clock::now();

//   uint64_t numberElements = segPerClient * (Config::DFI_FULL_SEGMENT_SIZE - sizeof(DFI_SEGMENT_HEADER_t)) / dataSize;

//   std::cout << "Starting to append" << '\n';

//   for (size_t i = 0; i < numberElements; i++)
//   {
//     buffWriter.add(scratchPad, dataSize);
//   }

//   buffWriter.close();

//   auto endTime = chrono::high_resolution_clock::now();

//   auto diff = chrono::duration_cast<chrono::milliseconds>(endTime - startTime);

//   *runConsumer = false;
//   consumer.join();

//   std::cout << "Time: " << diff.count() << " ms" << '\n';

//   ASSERT_EQ( (size_t)segPerClient, (size_t)*segmentsConsumed) << "Consumed number of segments did not match expected";
// }
