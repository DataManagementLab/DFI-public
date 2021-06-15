/** 
 * @file BufferHandle.h
 * @author cbinnig, lthostrup, tziegler
 * @date 2018-07-06
 */

#pragma once

#include "../../utils/Config.h"
#include "../../utils/Logging.h"
#include "local_iterators/SegmentIterator.h"
#include "local_iterators/BufferIterator.h"
#include "BufferSegment.h"
#include "FlowOptimization.h"

namespace dfi
{

struct BufferHandle
{
    enum Placement {
        CPU,
        GPU_DEVICE_TGT, //Buffer is on GPU and consumed from through device functions (i.e. gpu kernel)
        GPU_HOST_TGT, //Buffer is on GPU and consumed from through host functions (i.e. cpu)
    };
    
    string name;
    NodeID node_id;                           //Node id of where buffer resides
    std::vector<BufferSegment> entrySegments; //Each entry in the vector corresponds to one ring for a writer
    size_t segmentsPerWriter;                 //Number of segments in ring that will be created for each writer when createSegmentRingOnBuffer() is called
    size_t numberOfWriters;
    size_t segmentSizes;                      //in bytes (excluding header!)
    FlowOptimization buffertype;
    bool dataCacheAligned = false;
    char* localRdmaPtr = nullptr; //Pointer to start of rdma memory region on node where buffer resides.
    Placement placement = Placement::CPU;

    BufferHandle(){};
    BufferHandle(string name, NodeID node_id, size_t segmentsPerWriter, size_t numberOfWriters, size_t segmentSizes,
     FlowOptimization buffertype = FlowOptimization::BW, bool dataCacheAligned = false)
        : name(name), node_id(node_id), segmentsPerWriter(segmentsPerWriter), numberOfWriters(numberOfWriters), segmentSizes(segmentSizes), buffertype(buffertype), dataCacheAligned(dataCacheAligned){};

    //Returns BufferIteratorBW. Works only on local buffers!
    BufferIterator *getNewIterator(ConsumeScheme consumeScheme = ConsumeScheme::ASYNC)
    {
        switch(buffertype) {
            case FlowOptimization::BW:
            case FlowOptimization::LAT:
                return new BufferIterator(this->localRdmaPtr, entrySegments, consumeScheme, segmentsPerWriter);
            default:
                Logging::error(__FILE__, __LINE__, "getNewIterator on buffer: " + name + ", node-id: " + to_string(node_id) + ". BufferType not supported.");
                return nullptr;
        }
    }
private:
};


struct TargetOptions
{
    TargetID targetId;
    size_t full_segment_size;
    size_t segments_per_ring;
    BufferHandle::Placement target_placement;
    //# sources??
};


} // namespace dfi
