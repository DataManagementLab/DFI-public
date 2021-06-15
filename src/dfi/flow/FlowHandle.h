#pragma once

#include "../../utils/Config.h"
#include "../type/Schema.h"
#include "../../dfi/type/EnumTypes.h"
#include "../memory/BufferHandle.h"
#include "../memory/FlowOptimization.h"

namespace dfi
{
    
struct FlowHandle
{
    string name; //Used to identify the flow
    std::vector<source_t> sources; //Logical FlowSourceID mapping to (potential) NodeID. Note: not all flows requires an addressable NodeServer and therefore NodeID can be 0.
    std::vector<target_t> targets; //Logical FlowTargetID mapping to NodeID
    Schema schema;
    int groupKeyIndex;
    FlowType flowtype;
    uint32_t segmentsPerRing = Config::DFI_SEGMENTS_PER_RING;
    uint64_t segmentSizes = Config::DFI_FULL_SEGMENT_SIZE - sizeof(DFI_SEGMENT_FOOTER_t);
    FlowOptimization optimization = FlowOptimization::BW;
    AggrFunc aggrFunc = AggrFunc::NONE;
    std::string multicastAddress;
    uint64_t globalSeqNoOffset;
    std::vector<TargetOptions> targetSpecificOptions;

    FlowHandle() {}
    FlowHandle(string name) : name(name) {}
    FlowHandle(string name, std::vector<target_t> targets, Schema schema, size_t numberOfSources, uint32_t groupKeyIndex, FlowType flowtype, FlowOptimization opti): 
        FlowHandle{name, std::vector<source_t>(numberOfSources), targets, schema, groupKeyIndex, flowtype, opti} {
        }

    FlowHandle(string name, std::vector<source_t> sources, std::vector<target_t> targets, Schema schema, uint32_t groupKeyIndex, FlowType flowtype, FlowOptimization opti) 
        : name(name), sources(sources), targets(targets), schema(schema), groupKeyIndex(groupKeyIndex), flowtype(flowtype), optimization(opti) {
        }
};

}//dfi namespace end
