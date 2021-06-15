/**
 * @file Config.h
 * @author cbinnig, lthostrup, tziegler
 * @date 2018-08-17
 */



#ifndef DFI_CONFIG_HPP_
#define DFI_CONFIG_HPP_

//Includes
#include <iostream>
#include <stddef.h>
#include <sstream>
#include <unistd.h>
#include <stdint.h>
#include <stdexcept>
#include <vector>
#include <unordered_map>
#include <stdlib.h>
#include <fstream>
#include <google/protobuf/stubs/common.h>
#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>
#include <stdio.h>
#include <string.h> /* For strncpy */
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <netinet/in.h>
#include <net/if.h>
#include <arpa/inet.h>
#include <atomic>
#include "../rdma-manager/src/utils/Config.h"

using namespace std;

#define DFI_DEBUG_LEVEL 1

#ifdef DFI_DEBUG_LEVEL
#define DFI_DEBUG(X, ...) if(DFI_DEBUG_LEVEL>0) {fprintf(stdout, X, ##__VA_ARGS__); fflush(stdout);}
#else
#define DFI_DEBUG(X, ...) {}
#endif

//To be implemented MACRO
#define TO_BE_IMPLEMENTED(code_fragment)
#define DFI_UNIT_TEST_SUITE(suite) CPPUNIT_TEST_SUITE(suite)
#define DFI_UNIT_TEST(test) CPPUNIT_TEST(test)
#define DFI_UNIT_TEST_SUITE_END() CPPUNIT_TEST_SUITE_END()

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#define likely(x)    __builtin_expect (!!(x), 1)
#define unlikely(x)  __builtin_expect (!!(x), 0)

//typedefs
typedef unsigned long long uint128_t;
typedef uint64_t NodeID;
typedef uint64_t Offset;

typedef uint64_t TargetID;
typedef uint64_t SourceID;

namespace dfi
{


//FLOW TARGET (mapping from TargetID to NodeID)
struct target_t
{
    TargetID targetId;
    NodeID nodeId;
    target_t(TargetID const tid, NodeID const nid) : targetId(tid), nodeId(nid) {};
    target_t() {};
    bool operator < (target_t const& other) {
        return this->targetId < other.targetId;
    }
};

struct source_t {
    SourceID sourceId;
    NodeID nodeId;

    bool operator < (source_t const& other) {
        return this->sourceId < other.sourceId;
    }

    source_t() {};
    source_t(SourceID const sid) : sourceId(sid), nodeId(0) {};
    source_t(SourceID const sid, NodeID const nid) : sourceId(sid), nodeId(nid) {};
};


/**
 * @brief Config::DFI_MULTICAST_SEGMENT_HEADER_t describes the header of a multicast segment
 */
using seq_no_t = uint64_t;
struct DFI_MULTICAST_SEGMENT_HEADER_t
{
    union {
        seq_no_t seq_no; //for ordering
        size_t data_size; //for batching (bandwidth optimization)
    };
    bool is_last;
    SourceID source_id;
    
    DFI_MULTICAST_SEGMENT_HEADER_t(seq_no_t segment_idx, bool is_last, SourceID source_id) : seq_no(segment_idx), is_last(is_last), source_id(source_id) {}
};

using checksum_t = uint64_t;
/**
 * @brief DFI_SEGMENT_FOOTER_t describes the footer of a segment
 */
struct DFI_SEGMENT_FOOTER_t
{
#ifdef __NVCC__
volatile
#endif
    uint32_t counter = 0; //Counter must be first!
    volatile uint32_t segmentFlags = 0;
    DFI_SEGMENT_FOOTER_t(){};
    DFI_SEGMENT_FOOTER_t(uint32_t counter, uint32_t segmentFlags) : counter(counter), segmentFlags(segmentFlags){};

#ifdef __NVCC__
__host__ __device__
#endif
    bool isWriteable() const { return (segmentFlags & 1) != 0; } 
#ifdef __NVCC__
__host__ __device__
#endif
    void setWriteable(bool writeable) { segmentFlags = (writeable ? segmentFlags | 1 : segmentFlags & ~(1)); }

#ifdef __NVCC__
__host__ __device__
#endif
    bool isConsumable() const { return (segmentFlags & (1 << 1)) != 0; }
#ifdef __NVCC__
__host__ __device__
#endif
    void setConsumable(bool consumable) { segmentFlags = (consumable ? segmentFlags | (1 << 1) : segmentFlags & ~(1 << 1)); }
#ifdef __NVCC__
__host__ __device__
#endif
    bool isEndSegment() const { return (segmentFlags & (1 << 2)) != 0; }
#ifdef __NVCC__
__host__ __device__
#endif
    void markEndSegment(bool endSegment = true) { segmentFlags = (endSegment ? segmentFlags | (1 << 2) : segmentFlags & ~(1 << 2)); }

    friend std::ostream& operator<<(std::ostream &out, DFI_SEGMENT_FOOTER_t const& obj){
        out << "Counter: " << obj.counter << " IsWritable: " << obj.isWriteable() << " IsConsumable: " << obj.isConsumable() << " IsEndSegment: " << obj.isEndSegment();
        return out;
    }
};

//Constants
class Config
{
  public:
    Config(const std::string& exec_path, const std::string &config_filename = "")
    {
        load(exec_path, config_filename);

        rdma::Config::SEQUENCER_IP = Config::DFI_REGISTRY_SERVER; //Set SEQUENCER_IP to REGISTRY IP since its nested inside registry
    }

    ~Config()
    {
        unload();
    }

    //DFI
    const static int DFI_MAX_SOCKETS = 1024;
    const static int DFI_SLEEP_INTERVAL = 100 * 1000;
    
    static std::string DFI_REGISTRY_SERVER;
    static uint16_t DFI_REGISTRY_PORT;
    static uint16_t DFI_REGISTRY_RDMA_MEM;
    static std::vector<std::string> DFI_NODES;

    static bool DFI_CLEANUP_FLOWS;
    static bool PRINT_SOURCE_STATS;

    static uint16_t DFI_NODE_PORT;
    static size_t DFI_SOURCE_SEGMENT_COUNT; //Number of source segments for output buffer (per target)
    static size_t DFI_FULL_SEGMENT_SIZE;
    static size_t DFI_SEGMENTS_PER_RING;

    static std::vector<std::string> DFI_MULTICAST_ADDRS;

    static std::string getBufferName(std::string& flowName, TargetID target); 

    //SYSTEM
    static uint32_t CACHELINE_SIZE;

    //LOGGING
    static int LOGGING_LEVEL; //0=all, 1=ERR, 2=DBG, 3=INF, (>=4)=NONE

    static std::string& getIPFromNodeId(NodeID& nodeid);
    static std::string& getIPFromNodeId(const NodeID& nodeid);
  
  private:
    static void load(const std::string& exec_path, const std::string &config_filename);
    static void unload();

    static void set(std::string key, std::string value);
    static void init_vector(std::vector<std::string> &values, std::string csv_list);
    static void init_vector(std::vector<int> &values, std::string csv_list);
};

} // end namespace dfi

using namespace dfi;

#endif /* DFI_CONFIG_HPP_ */
