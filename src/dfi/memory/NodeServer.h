/**
 * @file NodeServer.h
 * @author cbinnig, lthostrup, tziegler
 * @date 2018-07-06
 */

#pragma once

#include "../../utils/Config.h"
#include "../../utils/Logging.h"
#include "../../rdma-manager/src/proto/ProtoServer.h"
#include "../../rdma-manager/src/rdma/RDMAServer.h"
#include <mutex> // For std::unique_lock
#include <shared_mutex>
#include "../message/MessageTypes.h"
#include "../message/MessageErrors.h"
#include <google/protobuf/empty.pb.h>

namespace dfi
{

class NodeServer : public rdma::RDMAServer<rdma::ReliableRDMA>
{

    struct AllocatedMemoryInfo
    {
        uint64_t size;
        uint64_t offset;
    };
  private:
    std::unordered_map<std::string, std::vector<AllocatedMemoryInfo>> allocatedMemoryInfoMap;


  public:
    NodeServer();
    NodeServer(uint64_t memsize);
    NodeServer(uint64_t memsize, uint16_t port);
    NodeServer(uint64_t memsize, uint16_t port, int numaNode);
    ~NodeServer() = default;

protected:
    void handle(Any *sendMsg, Any *respMsg) override
    {
        if (sendMsg->Is<DFIAllocMultipleSegmentsRequest>())
        {
            
            Logging::debug(__FILE__, __LINE__, "Got DFIAllocMultipleSegmentsRequest msg");
            DFIAllocMultipleSegmentsRequest reqMsgUnpacked;
            DFIAllocMultipleSegmentsResponse respMsgUnpacked;
            sendMsg->UnpackTo(&reqMsgUnpacked);
            for (int i = 0; i < reqMsgUnpacked.allocsegmentrequests_size(); i++)
            {
                DFIAllocSegmentsRequest allocReqMsgUnpacked = reqMsgUnpacked.allocsegmentrequests(i);
                auto response = respMsgUnpacked.add_responses();
                handleDFIAllocSegmentsRequest(allocReqMsgUnpacked, response);
                if (response->return_() != rdma::MessageErrors::NO_ERROR)
                {
                    Logging::error(__FILE__, __LINE__, "DFIAllocMultipleSegmentsRequest failed since segment couldn't be allocated!");
                    break;
                }
            }
            respMsg->PackFrom(respMsgUnpacked);
            Logging::debug(__FILE__, __LINE__, "Finished DFIAllocMultipleSegmentsRequest");
        }
        else if (sendMsg->Is<DFIAllocSegmentsRequest>())
        {
            Logging::debug(__FILE__, __LINE__, "Got DFIAllocSegmentsRequest msg");
            DFIAllocSegmentsRequest reqMsgUnpacked;
            DFIAllocSegmentsResponse respMsgUnpacked;

            sendMsg->UnpackTo(&reqMsgUnpacked);

            handleDFIAllocSegmentsRequest(reqMsgUnpacked, &respMsgUnpacked);

            respMsg->PackFrom(respMsgUnpacked);

            Logging::debug(__FILE__, __LINE__, "Finished DFIAllocSegmentsRequest");
        }
        else if(sendMsg->Is<DFIFreeSegmentsRequest>())
        {
            DFIFreeSegmentsRequest reqMsgUnpacked;
            sendMsg->UnpackTo(&reqMsgUnpacked);
            std::string name = reqMsgUnpacked.name();
            if (allocatedMemoryInfoMap.find(name) != allocatedMemoryInfoMap.end())
            {
                auto memInfos = allocatedMemoryInfoMap[name];
                try
                {
                    for (auto &memInfo : memInfos)
                    {
                        this->localFree(memInfo.offset);
                    }
                    allocatedMemoryInfoMap.erase(name);
                }
                catch(const std::runtime_error& e)
                {
                    Logging::warn("Freeing segment ring triggered exception: " + std::string(e.what()));
                }
            }

            google::protobuf::Empty emptyMsg;
            respMsg->PackFrom(emptyMsg);
        }
        else
        {
            rdma::RDMAServer<rdma::ReliableRDMA>::handle(sendMsg, respMsg);
        }
    }

    void handleDFIAllocSegmentsRequest(DFIAllocSegmentsRequest &reqMsgUnpacked, DFIAllocSegmentsResponse *respMsgUnpacked)
    {
        size_t segmentsCount = reqMsgUnpacked.segments_count();
        size_t fullSegmentsSize = reqMsgUnpacked.segments_size();
        string name = reqMsgUnpacked.name();
        size_t firstSegOffset = 0;
        FlowOptimization bufferType = static_cast<FlowOptimization>(reqMsgUnpacked.buffer_type());
        // std::cout << "segmentsCount: " << segmentsCount << " fullSegmentsSize: " << fullSegmentsSize << " name: " << name << std::endl;
        bool cacheAlign = reqMsgUnpacked.cache_align();
        if (cacheAlign)
        {
            Logging::error(__FILE__, __LINE__, "Cache aligning data portion of segments is not supported!");
            respMsgUnpacked->set_return_(MessageErrors::DFI_ALLOCATE_SEGMENT_FAILED);
            return;
        }

        size_t allocateMemorySize = segmentsCount * (fullSegmentsSize + (cacheAlign ? Config::CACHELINE_SIZE : 0));
        if (bufferType == FlowOptimization::LAT || bufferType == FlowOptimization::BW)
        {
            allocateMemorySize += sizeof(uint64_t); //Add the counter
        }
        Logging::debug(__FILE__, __LINE__, "Requesting Memory Resource, size: " + to_string(allocateMemorySize) + " cache align: " + to_string(cacheAlign));
        auto errorCode = requestMemoryResource(allocateMemorySize, firstSegOffset);


        respMsgUnpacked->set_return_(errorCode);
        
        if (errorCode != rdma::MessageErrors::NO_ERROR)
        {
            Logging::error(__FILE__, __LINE__, "Error occured when requesting memory resource for buffer: "+name+" (" + to_string(allocateMemorySize) + " byte), errcode: " + to_string(errorCode));
            return;
        }

        //save allocateMemorySize and firstSegOffset for flowname to later cleanup...
        allocatedMemoryInfoMap[name].push_back({allocateMemorySize, firstSegOffset});
        
        if (bufferType == FlowOptimization::LAT || bufferType == FlowOptimization::BW)
        {
            uint64_t counter = segmentsCount;
            memcpy(reinterpret_cast<uint8_t*>(this->getBuffer(0)) + firstSegOffset, &counter, sizeof(uint64_t));
            // std::cout << "Wrote counter on firstSegOffset: " << firstSegOffset << " initial value: " << counter << '\n';
            firstSegOffset += sizeof(int64_t); //shift the firstSegOffset past the counter
        }

        size_t prevSegOffset = firstSegOffset;
        for (size_t i = 0; i < segmentsCount; i++)
        {
            size_t curSegOffset = prevSegOffset + (i != 0 ? fullSegmentsSize : 0);
            // std::cout << "curSegOffset was: " << curSegOffset << '\n';
            // if (cacheAlign)
            // {
            //     size_t curSegDataOffset = curSegOffset;
            //     size_t space = fullSegmentsSize + Config::CACHELINE_SIZE;
            //     void* dataptr = (char *)this->getBuffer(curSegDataOffset);
            //     void* aligneddataptr = dataptr;

            //     //offset segment offset so it's data portion is cache aligned
            //     if (std::align(Config::CACHELINE_SIZE, fullSegmentsSize, aligneddataptr, space))
            //     {
            //         curSegOffset += (char*)aligneddataptr - (char*)dataptr;
            //     }
            //     else
            //     {
            //         Logging::error(__FILE__, __LINE__, "Error happened when aligning segment-memory (not header) to cachelines");
            //     }
            // }

            //Chain segments together to form a segment-ring
            if (i == 0)
                firstSegOffset = curSegOffset;
            else
            {   //Write to the last segment
                DFI_SEGMENT_FOOTER_t segmentHeader;
                segmentHeader.counter = 0;
                segmentHeader.setWriteable(true);
                //Write the header
                memcpy(this->getBuffer(prevSegOffset+fullSegmentsSize-sizeof(segmentHeader)), &segmentHeader, sizeof(segmentHeader));

                // memcpy((char *)this->getBuffer(prevSegOffset) + fullSegmentsSize - sizeof(segmentHeader), &segmentHeader, sizeof(segmentHeader));
                // std::cout << "wrote header to previous segment: " << prevSegOffset << '\n';
            }
            prevSegOffset = curSegOffset;
        }

        DFI_SEGMENT_FOOTER_t segmentHeader;
        segmentHeader.counter = 0;
        segmentHeader.setWriteable(true);
        //Write the header
        memcpy(this->getBuffer(prevSegOffset + fullSegmentsSize - sizeof(segmentHeader)), &segmentHeader, sizeof(segmentHeader)); 

        // std::cout << "wrote header to previous segment: " << prevSegOffset << '\n';

        respMsgUnpacked->set_offset(firstSegOffset);
    }
};

} // namespace dfi