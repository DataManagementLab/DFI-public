#include "../registry/RegistryClient.h"
#include <math.h>

RegistryClient::RegistryClient()
    : rdma::ProtoClient(), registry_ipPort(Config::DFI_REGISTRY_SERVER + ":" + to_string(Config::DFI_REGISTRY_PORT))
{
    // Logging::info("RegistryClient connecting to Registry Server on: " + registry_ipPort);
    if (!rdma::ProtoClient::connectProto(registry_ipPort))
    {
        throw invalid_argument("Can not connect to DFI registry");
    }
}

RegistryClient::~RegistryClient()
{
}

std::unique_ptr<BufferHandle> RegistryClient::retrieveBuffer(string &buffername, string flowname)
{
    Any sendAny = MessageTypes::createDFIRetrieveBufferRequest(buffername, flowname, false, false);

    auto buffHandle = retrieveOrJoinBuffer(&sendAny, buffername);
    return buffHandle;
}


std::unique_ptr<BufferHandle> RegistryClient::targetJoinBuffer(string &buffername, string &flowname, TargetID targetid){
    Any sendAny = MessageTypes::createDFIRetrieveBufferRequest(buffername, flowname, false, true, targetid);
    auto buffHandle = retrieveOrJoinBuffer(&sendAny, buffername);
    return buffHandle;
}

std::unique_ptr<BufferHandle> RegistryClient::sourceJoinBuffer(string &buffername, string flowname){
    Any sendAny = MessageTypes::createDFIRetrieveBufferRequest(buffername, flowname, true, false);
    auto buffHandle = retrieveOrJoinBuffer(&sendAny, buffername);
    return buffHandle;
}


std::unique_ptr<BufferHandle> RegistryClient::retrieveOrJoinBuffer(Any* sendAny, string &name){
        Any rcvAny;

    rdma::ProtoClient::exchangeProtoMsg(registry_ipPort, sendAny, &rcvAny);


    if (!rcvAny.Is<DFIRetrieveBufferResponse>())
    {
        Logging::error(__FILE__, __LINE__, "Unexpected response");
        return nullptr;
    }

    Logging::debug(__FILE__, __LINE__, "Received response");
    DFIRetrieveBufferResponse rtrvBufferResp;
    rcvAny.UnpackTo(&rtrvBufferResp);

    if (rtrvBufferResp.return_() == MessageErrors::DFI_NO_BUFFER_FOUND)
    {
        Logging::info("No BufferHandle found when retrieving BufferHandle from Registry");
        return nullptr;
    }
    if (rtrvBufferResp.return_() != MessageErrors::NO_ERROR)
    {
        Logging::error(__FILE__, __LINE__, "Error on server side, error code: " + to_string(rtrvBufferResp.return_()));
        return nullptr;
    }

    NodeID node_id = rtrvBufferResp.node_id();
    size_t segmentsPerWriter = rtrvBufferResp.segmentsperwriter();
    size_t segmentSizes = rtrvBufferResp.segmentsizes();
    size_t numberAppenders = rtrvBufferResp.segment_size();
    uint64_t localRdmaMem = rtrvBufferResp.local_rdma_ptr();
    bool cacheAligned = rtrvBufferResp.cache_align_data();
    FlowOptimization buffertype =  static_cast<FlowOptimization>(rtrvBufferResp.buffertype());
    auto buffHandle = std::make_unique<BufferHandle>(name, node_id, segmentsPerWriter, numberAppenders, segmentSizes, buffertype, cacheAligned);
    buffHandle->localRdmaPtr = (char*)localRdmaMem;
    for (size_t i = 0; i < numberAppenders; ++i)
    {
        DFIRetrieveBufferResponse_Segment segmentResp = rtrvBufferResp.segment(i);
        BufferSegment segment(segmentResp.offset(), segmentResp.size());
        buffHandle->entrySegments.push_back(segment);
    }

    return buffHandle;
}

//Change so private can use this instead of create buffer
bool RegistryClient::registerBuffer(BufferHandle *handle)
{
    Any sendAny = MessageTypes::createDFIRegisterBufferRequest(*handle);
    return appendOrRetrieveSegment(&sendAny);
}

// bool RegistryClient::appendSegment(string &name, BufferSegment &segment)
// {
//     Any sendAny = MessageTypes::createDFIAppendBufferRequest(name, segment.offset, segment.size, segment.nextSegmentOffset);
//     return appendOrRetrieveSegment(&sendAny);
// }

bool RegistryClient::appendOrRetrieveSegment(Any *sendAny)
{
    Any rcvAny;
    rdma::ProtoClient::exchangeProtoMsg(registry_ipPort, sendAny, &rcvAny);

    if (!rcvAny.Is<DFIAppendBufferResponse>())
    {
        Logging::error(__FILE__, __LINE__, "Unexpected response");
        return false;
    }

    Logging::debug(__FILE__, __LINE__, "Received response");
    DFIAppendBufferResponse appendBufferResp;
    rcvAny.UnpackTo(&appendBufferResp);
    if (appendBufferResp.return_() != MessageErrors::NO_ERROR)
    {
        Logging::error(__FILE__, __LINE__, "Error on server side, errcode: " + to_string(appendBufferResp.return_()));
        return false;
    }

    return true;
}

bool RegistryClient::createFlow(FlowHandle &flowHandle, bool cacheAlignSegments)
{
    Logging::debug(__FILE__, __LINE__, "RegistryClient creating flow: " + flowHandle.name);
    // std::cout << flowHandle.segmentSizes << " # " << flowHandle.schema.getTupleSize() << '\n';

    if (flowHandle.segmentSizes % flowHandle.schema.getTupleSize() != 0) {
        Logging::error(__FILE__, __LINE__, "Only segment payload sizes divisible with tuple sizes are supported. Tuple size: " + to_string(flowHandle.schema.getTupleSize()) + " segment payload size: " + to_string(flowHandle.segmentSizes));
        return false;
    }

    if (flowHandle.optimization == FlowOptimization::MULTICAST || flowHandle.optimization == FlowOptimization::MULTICAST_ORDERING || flowHandle.optimization == FlowOptimization::MULTICAST_BW)
    {
        //Check tuple size is smaller than MTU
        if (flowHandle.schema.getTupleSize()+sizeof(DFI_MULTICAST_SEGMENT_HEADER_t) > rdma::Config::RDMA_UD_MTU)
        {
            throw invalid_argument("Tuple sizes (+header) exceed the MTU limit of RDMA UD.");
        }
    }

    Any sendAny = MessageTypes::createDFICreateFlowRequest(flowHandle.name, flowHandle.sources, flowHandle.targets, flowHandle.schema, flowHandle.groupKeyIndex, flowHandle.flowtype, flowHandle.segmentsPerRing,flowHandle.segmentSizes, cacheAlignSegments,flowHandle.optimization, flowHandle.aggrFunc, flowHandle.targetSpecificOptions);
    Any rcvAny;

    rdma::ProtoClient::exchangeProtoMsg(registry_ipPort, &sendAny, &rcvAny);

    if (!rcvAny.Is<DFICreateFlowResponse>()) {
        Logging::error(__FILE__, __LINE__, "Unexpected response");
        return false;
    }

    Logging::debug(__FILE__, __LINE__, "Received response");
    DFICreateFlowResponse createFlowResp;
    rcvAny.UnpackTo(&createFlowResp);

    if (createFlowResp.return_() != MessageErrors::NO_ERROR) {
        Logging::error(__FILE__, __LINE__, "Error on server side");
        return false;
    }        
    
    // cout << flowHandle.numberOfSources << endl;
    // auto flowhandle = new FlowHandle(flowHandle.name,flowHandle.targets, flowHandle.schema,flowHandle.numberOfSources,flowHandle.groupKeyIndex,flowHandle.flowtype,flowHandle.segmentsPerRing,flowHandle.segmentSizes, flowHandle.optimization, flowHandle.aggrFunc);
    Logging::debug(__FILE__, __LINE__, "Returning flowhandle with target.size() " + to_string(flowHandle.targets.size()));
    return true;
}

std::unique_ptr<FlowHandle> RegistryClient::retrieveFlowHandle(string name)
{
    Any sendAny = MessageTypes::createDFIRetrieveFlowHandleRequest(name);
    Any rcvAny;

    rdma::ProtoClient::exchangeProtoMsg(registry_ipPort, &sendAny, &rcvAny);

    if (!rcvAny.Is<DFIRetrieveFlowHandleResponse>())
    {
        Logging::error(__FILE__, __LINE__, "Unexpected response");
        return nullptr;
    }

    DFIRetrieveFlowHandleResponse retrieveFlowResp;
    rcvAny.UnpackTo(&retrieveFlowResp);

    if (retrieveFlowResp.return_() == MessageErrors::DFI_NO_FLOW_FOUND)
    {
        Logging::info("No FlowHandle found when retrieving FlowHandle from Registry");
        return nullptr;
    }
    if (retrieveFlowResp.return_() != MessageErrors::NO_ERROR)
    {
        Logging::error(__FILE__, __LINE__, "Error on server side, errcode: " + to_string(retrieveFlowResp.return_()));
        return nullptr;
    }
    if ((size_t)retrieveFlowResp.schema_column_types_size() != (size_t)retrieveFlowResp.schema_column_names_size())
    {
        Logging::error(__FILE__, __LINE__, "Received protobuf msg: Number of names and types are not equal!");
        return nullptr;
    }
    if ((size_t)retrieveFlowResp.targets_size() != (size_t)retrieveFlowResp.target_nodeids_size())
    {
        Logging::error(__FILE__, __LINE__, "Received protobuf msg: Number of targets and nodeids are not equal!");
        return nullptr;
    }

    auto flowHandle = std::make_unique<FlowHandle>();
    flowHandle->name = name;
    for (size_t i = 0; i < (size_t)retrieveFlowResp.targets_size(); i++)
    {
        uint64_t targetId = retrieveFlowResp.targets(i);
        uint64_t nodeId = retrieveFlowResp.target_nodeids(i);
        flowHandle->targets.push_back(target_t(targetId, nodeId));
    }
    for (size_t i = 0; i < (size_t)retrieveFlowResp.sources_size(); i++)
    {
        uint64_t sourceId = retrieveFlowResp.sources(i);
        uint64_t nodeId = retrieveFlowResp.source_nodeids(i);
        flowHandle->sources.push_back(source_t(sourceId, nodeId));
    }
    
    std::vector<std::pair<std::string, TypeId>> columns;
    for (size_t i = 0; i < (size_t)retrieveFlowResp.schema_column_types_size(); i++)
    {
        columns.push_back(std::make_pair<std::string, TypeId>((string)retrieveFlowResp.schema_column_names(i), static_cast<TypeId>(retrieveFlowResp.schema_column_types(i))));
    }
    flowHandle->schema = Schema(columns);
    flowHandle->groupKeyIndex = retrieveFlowResp.grouping_key_index();
    flowHandle->flowtype = static_cast<FlowType>(retrieveFlowResp.flowtype());
    flowHandle->segmentSizes = retrieveFlowResp.segment_sizes();
    flowHandle->segmentsPerRing  = retrieveFlowResp.segments_per_ring();
    flowHandle->optimization = static_cast<FlowOptimization>(retrieveFlowResp.optimization());
    flowHandle->aggrFunc = static_cast<AggrFunc>(retrieveFlowResp.aggr_func());
    flowHandle->multicastAddress = retrieveFlowResp.multicast_address();
    flowHandle->globalSeqNoOffset = retrieveFlowResp.global_seq_offset();


    return flowHandle;
}


bool RegistryClient::getFlowBarrierOffset(std::string &name, size_t &offset)
{
    Any sendAny = MessageTypes::createDFIFlowBarrierRequest(name);
    Any rcvAny;

    try
    {
        rdma::ProtoClient::exchangeProtoMsg(registry_ipPort, &sendAny, &rcvAny);
    }
    catch(const std::runtime_error& e)
    {
        Logging::warn("RegistryClient getFlowBarrierOffset triggered exception: " + std::string(e.what()));
    }
    if (!rcvAny.Is<DFIFlowBarrierResponse>())
    {
        Logging::error(__FILE__, __LINE__, "Unexpected response");
        return false;
    }

    DFIFlowBarrierResponse flowBarrierResponse;
    rcvAny.UnpackTo(&flowBarrierResponse);

    if (flowBarrierResponse.return_() != MessageErrors::NO_ERROR)
    {
        Logging::error(__FILE__, __LINE__, "Error on server side, errcode: " + to_string(flowBarrierResponse.return_()));
        return false;
    }
    offset = flowBarrierResponse.offset();
    return true;
}



bool RegistryClient::destroyFlow(FlowHandle &handle)
{
    Any sendAny = MessageTypes::createDFIDestroyFlowRequest(handle.name);
    Any recvAny;
    try
    {
        rdma::ProtoClient::exchangeProtoMsg(registry_ipPort, &sendAny, &recvAny);
        
    }
    catch(const std::runtime_error& e)
    {
        Logging::warn("RegistryClient destroying flow triggered exception: " + std::string(e.what()));
        return false;
    }
    return true;    
}