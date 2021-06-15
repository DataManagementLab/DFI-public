

#ifndef DFI_MESSAGETYPES_H_
#define DFI_MESSAGETYPES_H_

#include "../../utils/Config.h"
#include "../../dfi/type/EnumTypes.h"

#include "DFICreateRingOnBufferRequest.pb.h"
#include "DFICreateRingOnBufferResponse.pb.h"
#include "DFIRetrieveBufferRequest.pb.h"
#include "DFIRetrieveBufferResponse.pb.h"
#include "DFIRetrieveFlowHandleRequest.pb.h"
#include "DFIRetrieveFlowHandleResponse.pb.h"
#include "DFIAppendBufferRequest.pb.h"
#include "DFIAppendBufferResponse.pb.h"
#include "DFIAllocSegmentsRequest.pb.h"
#include "DFIAllocSegmentsResponse.pb.h"
#include "DFICreateFlowRequest.pb.h"
#include "DFICreateFlowResponse.pb.h"
#include "DFIFlowBarrierRequest.pb.h"
#include "DFIFlowBarrierResponse.pb.h"
#include "DFIDestroyFlowRequest.pb.h"
#include "DFIFreeSegmentsRequest.pb.h"

#include "../../dfi/memory/BufferHandle.h"
#include "../../dfi/type/Schema.h"

#include <google/protobuf/any.pb.h>
#include <google/protobuf/message.h>
using google::protobuf::Any;

namespace dfi
{

enum MessageTypesEnum : int
{
  MEMORY_RESOURCE_REQUEST,
  MEMORY_RESOURCE_RELEASE,
};

class MessageTypes
{
public:


  static Any createDFIAllocSegmentsRequest(const string& bufferName, const size_t segmentsCount, const size_t segmentsSize, FlowOptimization buffertype = FlowOptimization::BW, bool cacheAlign = false)
  {
    DFIAllocSegmentsRequest allocSegReq;
    allocSegReq.set_name(bufferName);
    allocSegReq.set_segments_count(segmentsCount);
    allocSegReq.set_segments_size(segmentsSize);
    allocSegReq.set_buffer_type(buffertype);
    allocSegReq.set_cache_align(cacheAlign);
    Any anyMessage;
    anyMessage.PackFrom(allocSegReq);
    return anyMessage;
  }

  static Any createDFIAppendBufferRequest(string &name, size_t offset, size_t size, size_t threshold)
  {
    DFIAppendBufferRequest appendBufferReq;
    appendBufferReq.set_name(name);
    appendBufferReq.set_register_(false);

    DFIAppendBufferRequest_Segment *segmentReq = appendBufferReq.add_segment();
    segmentReq->set_offset(offset);
    segmentReq->set_size(size);
    segmentReq->set_threshold(threshold);
    Any anyMessage;
    anyMessage.PackFrom(appendBufferReq);
    return anyMessage;
  }

  static Any createDFIRegisterBufferRequest(BufferHandle& handle)
  {
    DFIAppendBufferRequest appendBufferReq;
    appendBufferReq.set_name(handle.name);
    appendBufferReq.set_node_id(handle.node_id);
    appendBufferReq.set_segmentsperwriter(handle.segmentsPerWriter);
    appendBufferReq.set_segmentsizes(handle.segmentSizes);
    appendBufferReq.set_numberappenders(handle.numberOfWriters);
    appendBufferReq.set_register_(true);
    appendBufferReq.set_buffertype(handle.buffertype);
    appendBufferReq.set_cache_align_data(handle.dataCacheAligned);
    
    // DFIAppendBufferRequest_Segment *segmentReq = appendBufferReq.add_segment();
    Any anyMessage;
    anyMessage.PackFrom(appendBufferReq);
    return anyMessage;
  }

  static Any createDFIRetrieveFlowHandleRequest(string &name)
  {
    DFIRetrieveFlowHandleRequest retrieveFlowHandleReq;
    retrieveFlowHandleReq.set_name(name);

    Any anyMessage;
    anyMessage.PackFrom(retrieveFlowHandleReq);
    return anyMessage;
  }

  static Any createDFIFlowBarrierRequest(string &name)
  {
    DFIFlowBarrierRequest flowBarrierRequest;
    flowBarrierRequest.set_name(name);

    Any anyMessage;
    anyMessage.PackFrom(flowBarrierRequest);
    return anyMessage;
  }

  static Any createDFIDestroyFlowRequest(string &name)
  {
    DFIDestroyFlowRequest destroyFlowRequest;
    destroyFlowRequest.set_name(name);

    Any anyMessage;
    anyMessage.PackFrom(destroyFlowRequest);
    return anyMessage;
  }

  static Any createDFICreateFlowRequest(string &name, std::vector<source_t> &sources, std::vector<target_t> &targets, Schema &schema, 
    uint32_t groupingKeyIndex, FlowType flowtype, uint32_t segmentsPerRing, uint64_t segmentSizes, bool cacheAlignSegments, 
    FlowOptimization opti, AggrFunc aggrFunc, std::vector<TargetOptions> targetSpecificOptions)
  {
    DFICreateFlowRequest createFlowRequest;
    createFlowRequest.set_name(name);
    createFlowRequest.set_segments_per_ring(segmentsPerRing);

    for (auto &source : sources) {
      createFlowRequest.add_sources(source.sourceId);
      createFlowRequest.add_source_nodeids(source.nodeId);
    }

    for (auto &target : targets)
    {
      createFlowRequest.add_targets(target.targetId);
      createFlowRequest.add_target_nodeids(target.nodeId);
    }
    for (size_t i = 0; i < schema.getColumnCount(); ++i)
    {
      createFlowRequest.add_schema_column_types(static_cast<uint32_t>(schema.getTypeForColumn(i)));
      createFlowRequest.add_schema_column_names(schema.getNameForColumn(i));
    }
    createFlowRequest.set_grouping_key_index(groupingKeyIndex);
    createFlowRequest.set_flowtype(flowtype);
    createFlowRequest.set_cache_align_segs(cacheAlignSegments);
    createFlowRequest.set_segment_sizes(segmentSizes);
    createFlowRequest.set_optimization(opti);
    createFlowRequest.set_aggr_func(aggrFunc);
    for (auto &targetOption : targetSpecificOptions)
    {
        auto option = createFlowRequest.add_target_options();
        option->set_target(targetOption.targetId);
        option->set_full_segment_size(targetOption.full_segment_size);
        option->set_segments_per_ring(targetOption.segments_per_ring);
        option->set_target_placement(static_cast<uint32_t>(targetOption.target_placement));
    }
    
    Any anyMessage;
    anyMessage.PackFrom(createFlowRequest);
    return anyMessage;
  }

  
  static Any createDFIRetrieveBufferRequest(string &buffername, string &flowname, bool isSource, bool isTarget, TargetID targetid = 0)
  {
    DFIRetrieveBufferRequest retrieveBufferReq;
    retrieveBufferReq.set_buffername(buffername);
    retrieveBufferReq.set_flowname(flowname);
    retrieveBufferReq.set_issource(isSource);
    retrieveBufferReq.set_istarget(isTarget);
    retrieveBufferReq.set_targetid(targetid);

    Any anyMessage;
    anyMessage.PackFrom(retrieveBufferReq);
    return anyMessage;
  }
};
// end class
} // end namespace dfi

#endif /* DFI_MESSAGETYPES_H_ */
