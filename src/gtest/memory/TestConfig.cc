#include "TestConfig.h"
#include <bits/stdc++.h> 
#include <iostream> 
#include <sys/stat.h> 
#include <sys/types.h> 

void TestConfig::SetUp()
{
  //Create new test config file
  if (mkdir(program_name.c_str(), 0777) == -1)
  {
    std::cerr << "Could not create test directory!" << '\n';
    std::cerr << "Error:  " << strerror(errno) << std::endl;   
  }  
  string subdir = program_name + "/conf";
  if (mkdir(subdir.c_str(), 0777) == -1)
  {
    std::cerr << "Could not create test directory!" << '\n';
    std::cerr << "Error:  " << strerror(errno) << std::endl;   
  }  
  
  std::ofstream confFile (program_name + "/conf/DFI.conf", std::ofstream::out);

  confFile << "LOGGING_LEVEL=1 \n\
RDMA_MEMSIZE = 1234567 \n\
DFI_REGISTRY_SERVER = 10.116.60.16 \n\
DFI_REGISTRY_PORT = 5300 \n\
THREAD_CPUS = 10,11,12,13,   14,15,16, 17,18,19 \n\
DFI_NODES = 10.116.60.16:1234,10.116.60.7:1234 \n\
CACHELINE_SIZE = 64 \n\
DFI_INTERNAL_BUFFER_SIZE = 654321 \n\
DFI_FULL_SEGMENT_SIZE = 234567 \n\
DFI_SEGMENTS_PER_RING = 99 \n\
  " << std::endl;

  confFile.close();
}

void TestConfig::TearDown()
{
  // REMOVE FILE AND FOLDERS
  string filename = program_name + "/conf/DFI.conf";
  std::remove(filename.c_str());
  string subfolder = program_name + "/conf";
  rmdir(subfolder.c_str());
  rmdir(program_name.c_str());

  //Reload the normal test config
  static Config conf("");
}


TEST_F(TestConfig, loadConfigFile)
{
  static Config conf(program_name+"/");
  
  ASSERT_TRUE( Config::LOGGING_LEVEL == 1) << "LOGGING_LEVEL";
  ASSERT_TRUE( Config::DFI_REGISTRY_SERVER == "10.116.60.16") << "DFI_REGISTRY_SERVER";
  ASSERT_TRUE( Config::DFI_NODES[0] == "10.116.60.16:1234") << "DFI_NODES";
  ASSERT_TRUE( Config::DFI_NODES[1] == "10.116.60.7:1234") << "DFI_NODES";
  ASSERT_TRUE( Config::CACHELINE_SIZE == 64) << "CACHELINE_SIZE";
  ASSERT_TRUE( Config::DFI_SOURCE_SEGMENT_COUNT == 654321) << "DFI_INTERNAL_BUFFER_SIZE";
  ASSERT_TRUE( Config::DFI_FULL_SEGMENT_SIZE == 234567) << "DFI_FULL_SEGMENT_SIZE";
  ASSERT_TRUE( Config::DFI_SEGMENTS_PER_RING == 99) << "DFI_SEGMENTS_PER_RING";

}

