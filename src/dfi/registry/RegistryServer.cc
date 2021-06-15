#include "RegistryServer.h"
#include <google/protobuf/empty.pb.h>

RegistryServer::RegistryServer(size_t mem_size) : RDMAServer("Registry Server", Config::DFI_REGISTRY_PORT, mem_size)
{
    nodeIDSequencer = std::make_unique<rdma::NodeIDSequencer>();

    m_nodeClient = std::make_unique<NodeClient>(mem_size);
    
    RDMAServer::startServer();
};

void RegistryServer::handle(Any *anyReq, Any *anyResp)
{
    if (anyReq->Is<DFIRetrieveBufferRequest>())
    {
        DFIRetrieveBufferRequest retrieveBuffReq;
        DFIRetrieveBufferResponse retrieveBuffResp;
        anyReq->UnpackTo(&retrieveBuffReq);
        
        this->handleDFIRetrieveBufferRequest(retrieveBuffReq, retrieveBuffResp);

        anyResp->PackFrom(retrieveBuffResp);
    }
    else if (anyReq->Is<DFIAppendBufferRequest>())
    {
        DFIAppendBufferRequest appendBuffReq;
        DFIAppendBufferResponse appendBuffResp;
        anyReq->UnpackTo(&appendBuffReq);

        this->handleDFIAppendBufferRequest(appendBuffReq, appendBuffResp);

        anyResp->PackFrom(appendBuffResp);
    }
    else if (anyReq->Is<DFICreateFlowRequest>())
    {
        DFICreateFlowRequest reqMsgUnpacked;
        DFICreateFlowResponse respMsgUnpacked;
        anyReq->UnpackTo(&reqMsgUnpacked);
        
        this->handleDFICreateFlowRequest(reqMsgUnpacked, respMsgUnpacked);

        anyResp->PackFrom(respMsgUnpacked);
    }
    else if (anyReq->Is<DFIRetrieveFlowHandleRequest>())
    {
        DFIRetrieveFlowHandleRequest reqMsgUnpacked;
        DFIRetrieveFlowHandleResponse respMsgUnpacked;
        anyReq->UnpackTo(&reqMsgUnpacked);
        
        this->handleDFIRetrieveFlowHandleRequest(reqMsgUnpacked, respMsgUnpacked);

        anyResp->PackFrom(respMsgUnpacked);
    }
    else if (anyReq->Is<DFIFlowBarrierRequest>())
    {
        DFIFlowBarrierRequest reqMsgUnpacked;
        DFIFlowBarrierResponse respMsgUnpacked;
        anyReq->UnpackTo(&reqMsgUnpacked);
        
        this->handleDFIFlowBarrierRequest(reqMsgUnpacked, respMsgUnpacked);

        anyResp->PackFrom(respMsgUnpacked);
    }
    else if (anyReq->Is<DFIDestroyFlowRequest>())
    {
        // std::cout << "Got DFIDestroyFlowRequest" << std::endl;
        DFIDestroyFlowRequest reqMsgUnpacked;
        anyReq->UnpackTo(&reqMsgUnpacked);
        
        this->handleDFIDestroyFlowRequest(reqMsgUnpacked);

        google::protobuf::Empty emptyMsg;
        anyResp->PackFrom(emptyMsg);
    }
    else
    {
        RDMAServer::handle(anyReq, anyResp);
        // Logging::error(__FILE__, __LINE__, "Registry Server received unrecognized proto-message");
        // ErrorMessage errorResp;
        // errorResp.set_return_(MessageErrors::INVALID_MESSAGE);
        // anyResp->PackFrom(errorResp);
    }
}

void RegistryServer::handleDFIRetrieveBufferRequest(DFIRetrieveBufferRequest &req, DFIRetrieveBufferResponse &resp)
{
    string name = req.buffername();
    BufferHandle *buffHandle = retrieveBuffer(name);
    if (buffHandle == nullptr)
    {
        resp.set_return_(MessageErrors::DFI_NO_BUFFER_FOUND);
        return;
    }
    // if (req.isTarget())
    // {
        
    // }

    if (req.issource())
    {
        auto &numberAppenders = m_appendersJoinedBuffer[name];
        if (numberAppenders == buffHandle->entrySegments.size())
        {
            resp.set_return_(MessageErrors::DFI_JOIN_BUFFER_FAILED);
            Logging::error(
                __FILE__,
                __LINE__,
                "DFI Join Buffer failed, all rings are in use. Increase number of appenders in creation of Buffer ");
        }
        else
        {
            resp.set_name(name);
            resp.set_node_id(buffHandle->node_id);
            BufferSegment BufferSegment = buffHandle->entrySegments[numberAppenders];
            DFIRetrieveBufferResponse_Segment *segmentResp = resp.add_segment();
            segmentResp->set_offset(BufferSegment.offset);
            segmentResp->set_size(BufferSegment.size);
            resp.set_segmentsizes(buffHandle->segmentSizes);
            resp.set_segmentsperwriter(buffHandle->segmentsPerWriter);
            resp.set_buffertype(buffHandle->buffertype);
            resp.set_cache_align_data(buffHandle->dataCacheAligned);
            resp.set_local_rdma_ptr((uint64_t)buffHandle->localRdmaPtr);
            ++numberAppenders;
            resp.set_return_(MessageErrors::NO_ERROR);
        }        
    }
    else
    {
        resp.set_name(name);
        resp.set_node_id(buffHandle->node_id);
        for (BufferSegment BufferSegment : buffHandle->entrySegments)
        {
            DFIRetrieveBufferResponse_Segment *segmentResp = resp.add_segment();
            segmentResp->set_offset(BufferSegment.offset);
            segmentResp->set_size(BufferSegment.size);
        }
        resp.set_segmentsizes(buffHandle->segmentSizes);
        resp.set_segmentsperwriter(buffHandle->segmentsPerWriter);
        resp.set_buffertype(buffHandle->buffertype);
        resp.set_local_rdma_ptr((uint64_t)buffHandle->localRdmaPtr);
        resp.set_return_(MessageErrors::NO_ERROR);
    }
}


void RegistryServer::handleDFIAppendBufferRequest(DFIAppendBufferRequest &req, DFIAppendBufferResponse &resp)
{
    bool registerSuccess = true;
    string name = req.name();
    size_t segmentsPerWriter = req.segmentsperwriter();
    size_t segmentSizes = req.segmentsizes();
    size_t numberAppenders = req.numberappenders();
    FlowOptimization bufferType = static_cast<FlowOptimization>(req.buffertype());
    bool cacheAlignData = req.cache_align_data();

    if (req.register_())
    {
        if (req.node_id() > Config::DFI_NODES.size())
        {
            Logging::error(__FILE__, __LINE__, "Node id ("+to_string(req.node_id())+") of buffer handle refers to unknown node (not specified in Config::DFI_NODES");
            resp.set_return_(MessageErrors::DFI_REGISTER_BUFFHANDLE_FAILED);
            return;
        }
        BufferHandle buffHandle(name, req.node_id(), segmentsPerWriter, numberAppenders, segmentSizes, bufferType, cacheAlignData);
        
        registerSuccess = registerBuffer(&buffHandle);
        
        BufferHandle *buffHandlePtr = retrieveBuffer(name);
        if (registerSuccess)
        {
            // std::cout << "buffHandlePtr->node_id " << buffHandlePtr->node_id<< std::endl;
            if (!m_nodeClient->isConnected(Config::getIPFromNodeId(buffHandlePtr->node_id)))
            {
                m_nodeClient->connect(Config::getIPFromNodeId(buffHandlePtr->node_id), buffHandlePtr->node_id);
            }
            buffHandlePtr->localRdmaPtr = (char*)m_nodeClient->getStartRdmaAddrForNode(buffHandlePtr->node_id);
            // Creating as many rings as appenders
            Logging::debug(__FILE__, __LINE__, "Register Called: Creating buffer rings on node: " + to_string(buffHandlePtr->node_id));
            auto segments = createMultipleRingsOnBuffer(buffHandlePtr, numberAppenders);
            if (segments.size() != numberAppenders)
            {
                registerSuccess = false;
            }

            buffHandlePtr->entrySegments = segments;
        }
        m_appendersJoinedBuffer[name] = 0;

        if (registerSuccess)
        {
            resp.set_return_(MessageErrors::NO_ERROR);
        }
        else
        {
            resp.set_return_(MessageErrors::DFI_REGISTER_BUFFHANDLE_FAILED);
        }
        return;
    }

    bool appendSuccess = true;
    if (registerSuccess)
    {
        for (int64_t i = 0; i < req.segment_size(); ++i)
        {
            DFIAppendBufferRequest_Segment segmentReq = req.segment(i);
            BufferSegment segment(segmentReq.offset(), segmentReq.size());
            if (!appendSegment(name, &segment))
            {
                appendSuccess = false;
                break;
            }
        }
    }

    if (appendSuccess)
    {
        resp.set_return_(MessageErrors::NO_ERROR);
    }
    else
    {
        resp.set_return_(MessageErrors::DFI_APPEND_BUFFHANDLE_FAILED);
    }
}  

void RegistryServer::handleDFICreateFlowRequest(DFICreateFlowRequest &req, DFICreateFlowResponse &resp)
{
    Logging::debug(__FILE__, __LINE__, "Got DFICreateFlowRequest msg");
    bool success = true;

    string flowName = req.name();
    FlowHandle flowHandle;
    flowHandle.name = flowName;
    uint32_t segmentsPerRing = req.segments_per_ring();
    uint64_t segmentsSizes = req.segment_sizes();
    FlowOptimization optimization = static_cast<FlowOptimization>(req.optimization());
    AggrFunc aggrFunc = static_cast<AggrFunc>(req.aggr_func());

    for (int i = 0; i < req.sources_size(); i++)
    {
        flowHandle.sources.emplace_back(req.sources(i), req.source_nodeids(i));
    }

    for (int i = 0; i < req.targets_size(); i++)
    {
        flowHandle.targets.emplace_back(req.targets(i), req.target_nodeids(i));
    }
    std::map<TargetID, TargetOptions> target_specific_options_map;
    for (int i = 0; i < req.target_options_size(); i++)
    {
        TargetOptions options;
        auto reqOptions = req.target_options(i);
        options.targetId = reqOptions.target();
        options.full_segment_size = reqOptions.full_segment_size();
        options.segments_per_ring = reqOptions.segments_per_ring();
        options.target_placement = static_cast<BufferHandle::Placement>(reqOptions.target_placement());
        flowHandle.targetSpecificOptions.push_back(options);
        target_specific_options_map[options.targetId] = options;
    }

    if (optimization == FlowOptimization::BW || optimization == FlowOptimization::LAT) {
        for (auto &target : flowHandle.targets)
        {
            // std::cout << "Registry Server creating Buffer for target: "<< targetId << " on node: " << nodeId << std::endl;
            bool cacheAlignSegments = req.cache_align_segs();
            //Create the DFI Buffers
            string bufferName = Config::getBufferName(flowName, target.targetId);
            
            BufferHandle bufferHandle(bufferName, target.nodeId, segmentsPerRing, flowHandle.sources.size(), segmentsSizes, optimization, cacheAlignSegments);
            
            if (target_specific_options_map.find(target.targetId) != target_specific_options_map.end())
            {
                auto &targetOptions = target_specific_options_map[target.targetId];
                bufferHandle.segmentSizes = targetOptions.full_segment_size - sizeof(DFI_SEGMENT_FOOTER_t);
                bufferHandle.segmentsPerWriter = targetOptions.segments_per_ring;
                bufferHandle.placement = targetOptions.target_placement;
            }

            DFIAppendBufferRequest appendBufferReq;
            DFIAppendBufferResponse appendBufferResp;
            appendBufferReq.set_name(bufferHandle.name);
            appendBufferReq.set_node_id(bufferHandle.node_id);
            appendBufferReq.set_segmentsperwriter(bufferHandle.segmentsPerWriter);
            appendBufferReq.set_segmentsizes(bufferHandle.segmentSizes);
            appendBufferReq.set_numberappenders(bufferHandle.numberOfWriters);
            appendBufferReq.set_register_(true);
            appendBufferReq.set_buffertype(bufferHandle.buffertype);
            appendBufferReq.set_cache_align_data(bufferHandle.dataCacheAligned);
            handleDFIAppendBufferRequest(appendBufferReq, appendBufferResp);

            if (appendBufferResp.return_() != MessageErrors::NO_ERROR)
            {
                Logging::error(__FILE__, __LINE__, "Could not create buffer for flow! MessageError: " + to_string(appendBufferResp.return_()));
                success = false;
                break;
            }
        }
    }
    else //Multicast
    {
        if (Config::DFI_MULTICAST_ADDRS.empty())
        {
            Logging::error(__FILE__, __LINE__, "Could not get multicast address for flow (Config::DFI_MULTICAST_ADDRS was empty)");
            success = false;
        }
        flowHandle.multicastAddress = Config::DFI_MULTICAST_ADDRS.back();
        // std::cout << "multicast addr: " << flowHandle.multicastAddress << std::endl;
        Config::DFI_MULTICAST_ADDRS.pop_back();

        if (optimization == FlowOptimization::MULTICAST_ORDERING)
        {
            //Allocate sequence number for fetch & add
            auto mem_resource = RDMAServer::internalAlloc(rdma::Config::CACHELINE_SIZE * 2); // allocate extra cacheline to align and prevent false sharing
            auto offset = mem_resource.offset + rdma::Config::CACHELINE_SIZE; // place uint64_t counter in the middle of
            uintptr_t ptr = reinterpret_cast<uintptr_t>((reinterpret_cast<uint8_t*>(getBuffer(0)) + offset));
            auto shift = rdma::Config::CACHELINE_SIZE - ptr % 64;
            offset += shift; // cache alignment
            ptr += shift;

            if (!mem_resource.isnull)
            {
                *reinterpret_cast<uint64_t*>(ptr) = 0;
                flowHandle.globalSeqNoOffset = offset;
            }
            else
            {
                Logging::error(__FILE__, __LINE__, "Registry server could not allocate RDMA memory for multicast sequence number");
                success = false;
            }
        }
    }
    
    
    
    if ((size_t)req.schema_column_types_size() != (size_t)req.schema_column_names_size())
    {
        Logging::error(__FILE__, __LINE__, "Number of names and types are not equal!");
        success = false;
    }
    std::vector<std::pair<std::string, TypeId>> columns;
    for (size_t i = 0; i < (size_t)req.schema_column_types_size(); i++)
    {
        columns.push_back(std::make_pair<std::string, TypeId>((string)req.schema_column_names(i), static_cast<TypeId>(req.schema_column_types(i))));
    }
    flowHandle.schema = Schema(columns);
    flowHandle.groupKeyIndex = req.grouping_key_index();
    flowHandle.segmentSizes = segmentsSizes;
    flowHandle.segmentsPerRing = segmentsPerRing;
    flowHandle.optimization = optimization;
    flowHandle.flowtype = static_cast<FlowType>(req.flowtype());
    flowHandle.aggrFunc = aggrFunc;
    if (success)
    {
        m_flowHandles[flowName] = flowHandle;
        m_activeTargets[flowName] = std::vector(flowHandle.targets.size(), false);
        resp.set_return_(MessageErrors::NO_ERROR);
    }
    else
        resp.set_return_(MessageErrors::DFI_CREATE_FLOW_FAILED);
}


void RegistryServer::handleDFIRetrieveFlowHandleRequest(DFIRetrieveFlowHandleRequest &req, DFIRetrieveFlowHandleResponse &resp)
{
    string name = req.name();
    auto flowHandle = this->retrieveFlow(name);

    if (flowHandle == nullptr)
    {
        resp.set_return_(MessageErrors::DFI_NO_FLOW_FOUND);
        return;
    }

    resp.set_name(name);
    for(size_t i = 0; i < flowHandle->sources.size(); i++)
    {
        resp.add_sources(flowHandle->sources[i].sourceId);
        resp.add_source_nodeids(flowHandle->sources[i].nodeId);
    }

    for(size_t i = 0; i < flowHandle->targets.size(); i++)
    {
        resp.add_targets(flowHandle->targets[i].targetId);
        resp.add_target_nodeids(flowHandle->targets[i].nodeId);
    }
    
    for(size_t i = 0; i < flowHandle->schema.getColumnCount(); i++)
    {
        resp.add_schema_column_names(flowHandle->schema.getNameForColumn(i));
        resp.add_schema_column_types(static_cast<uint32_t>(flowHandle->schema.getTypeForColumn(i)));
    }
    resp.set_grouping_key_index(flowHandle->groupKeyIndex);
    resp.set_segment_sizes(flowHandle->segmentSizes);
    resp.set_segments_per_ring(flowHandle->segmentsPerRing);
    resp.set_flowtype(flowHandle->flowtype);
    resp.set_optimization(flowHandle->optimization);
    resp.set_aggr_func(flowHandle->aggrFunc);
    resp.set_multicast_address(flowHandle->multicastAddress);
    resp.set_global_seq_offset(flowHandle->globalSeqNoOffset);
    resp.set_return_(MessageErrors::NO_ERROR);
}

void RegistryServer::handleDFIFlowBarrierRequest(DFIFlowBarrierRequest &req, DFIFlowBarrierResponse &resp)
{
    //Check that flow exists and barrier is not already initialized
    std::string name = req.name();

    if (m_flowHandles.find(name) == m_flowHandles.end())
    {
        Logging::error(__FILE__, __LINE__, "Registry server could find flow handle for flow barrier. Name: " + name);
        resp.set_return_(MessageErrors::DFI_NO_FLOW_FOUND);
        return;
    }

    if (m_flowBarrierOffsets.find(name) == m_flowBarrierOffsets.end())
    {
        //Allocate atomic counter
        auto mem_resource = RDMAServer::internalAlloc(rdma::Config::CACHELINE_SIZE * 2); // allocate extra cacheline to align and prevent false sharing
        auto offset = mem_resource.offset + rdma::Config::CACHELINE_SIZE; // place uint64_t counter in the middle of
        uintptr_t ptr = reinterpret_cast<uintptr_t>((reinterpret_cast<uint8_t*>(getBuffer(0)) + offset));
        auto shift = rdma::Config::CACHELINE_SIZE - ptr % 64;
        offset += shift; // cache alignment
        ptr += shift;

        if (!mem_resource.isnull)
        {
            *reinterpret_cast<uint64_t*>(ptr) = 0;
            m_flowBarrierOffsets[name] = offset;
        }
        else
        {
            Logging::error(__FILE__, __LINE__, "Registry server could not allocate RDMA memory for flow barrier");
            resp.set_return_(MessageErrors::DFI_INSUFFICIENT_MEM);
            return;
        }
    }

    resp.set_offset(m_flowBarrierOffsets[name]);
    resp.set_return_(MessageErrors::NO_ERROR);
}

void RegistryServer::handleDFIDestroyFlowRequest(DFIDestroyFlowRequest &req)
{
    std::string flowName = req.name();

    if (m_flowHandles.find(flowName) == m_flowHandles.end())
    {
        return; //Flow was already destroyed, nothing to do...
    }

    auto &activeTargets = m_activeTargets[flowName];
    activeTargets[req.targetid()] = false;

    if (std::all_of(activeTargets.begin(), activeTargets.end(), [](bool active){return !active;}))
    {

        auto &flowHandle = m_flowHandles[flowName];

        //clear buffer handles and remote buffers on DFI_NODEs

        for (auto &target : flowHandle.targets)
        {
            if (target.nodeId != 0) //If nodeid == 0, targets are not associated with any DFI_Nodes
            {
                string bufferName = Config::getBufferName(flowName, target.targetId);
                m_bufferHandles.erase(bufferName);
            }
        }

        //clear potential flow barrier offset entries
        m_flowBarrierOffsets.erase(flowName);

        //clear flow handle
        m_flowHandles.erase(flowName);
        Logging::info("Flow " + flowName + " finalized on RegistryServer");

    }
}

bool RegistryServer::registerBuffer(BufferHandle *buffHandle)
{
    if (m_bufferHandles.find(buffHandle->name) != m_bufferHandles.end())
    {
        Logging::error(__FILE__, __LINE__, "Buffer already exists on RegistryServer. Could not register. Name: " + buffHandle->name);
        return false;
    }
    m_bufferHandles[buffHandle->name] = *buffHandle;
    return true;
}

std::vector<BufferSegment> RegistryServer::createMultipleRingsOnBuffer(BufferHandle *bufferHandle, size_t count)
{
    std::vector<BufferSegment> bufferSegments;
    if (bufferHandle->node_id > Config::DFI_NODES.size())
    {
        return std::vector<BufferSegment>();
    }

    size_t offset = 0;
    string connection = Config::getIPFromNodeId(bufferHandle->node_id);
    size_t fullSegmentSize = bufferHandle->segmentSizes + sizeof(DFI_SEGMENT_FOOTER_t);
    std::vector<size_t> offsets;
    if (!m_nodeClient->remoteAllocSegments(connection, bufferHandle->node_id, bufferHandle->name, bufferHandle->segmentsPerWriter, fullSegmentSize, bufferHandle->buffertype, bufferHandle->dataCacheAligned, offsets, count))
    {
        Logging::error(__FILE__, __LINE__, "Could not remote allocate segment on buffer: " + bufferHandle->name + ". Requested size: " + to_string(fullSegmentSize));
        return std::vector<BufferSegment>();
    }

    Logging::debug(__FILE__, __LINE__, "Created ring with offset on entry segment: " + to_string(offset));

    for (size_t i = 0; i < count; i++)
    {
        bufferSegments.push_back(BufferSegment{offsets[i], bufferHandle->segmentSizes});
    }
    
    return bufferSegments;
    
}

BufferSegment RegistryServer::createRingOnBuffer(BufferHandle *bufferHandle)
{
    if (bufferHandle->node_id > Config::DFI_NODES.size())
    {
        return BufferSegment();
    }

    size_t offset = 0;
    string connection = Config::getIPFromNodeId(bufferHandle->node_id);
    size_t fullSegmentSize = bufferHandle->segmentSizes + sizeof(DFI_SEGMENT_FOOTER_t);
    std::vector<size_t> offsets;
    if (!m_nodeClient->remoteAllocSegments(connection, bufferHandle->node_id, bufferHandle->name, bufferHandle->segmentsPerWriter, fullSegmentSize, bufferHandle->buffertype, bufferHandle->dataCacheAligned, offsets, 1))
    {
        Logging::error(__FILE__, __LINE__, "Could not remote allocate segment on buffer: " + bufferHandle->name + ". Requested size: " + to_string(fullSegmentSize));
        return BufferSegment();
    }

    Logging::debug(__FILE__, __LINE__, "Created ring with offset on entry segment: " + to_string(offset));

    return BufferSegment{offset, bufferHandle->segmentSizes};
}

BufferHandle *RegistryServer::retrieveBuffer(string &name)
{
    if (m_bufferHandles.find(name) == m_bufferHandles.end())
    {
        Logging::info("RegistryServer: Could not find Buffer Handle for buffer: " + name);
        return nullptr;
    }
    return &m_bufferHandles[name];
}


FlowHandle *RegistryServer::retrieveFlow(string &name)
{
    if (m_flowHandles.find(name) == m_flowHandles.end())
    {
        Logging::info("RegistryServer: Could not find FlowHandle for flow: " + name);
        return nullptr;
    }
    return &m_flowHandles[name]; 
}

bool RegistryServer::appendSegment(string &name, BufferSegment *segment)
{
    if (m_bufferHandles.find(name) == m_bufferHandles.end())
    {
        return false;
    }
    m_bufferHandles[name].entrySegments.push_back(*segment);
    DFI_DEBUG("Registry Server: Appended new segment to %s \n", name.c_str());
    DFI_DEBUG("Segment offset: %zu, size: %zu \n", (*segment).offset, (*segment).size);
    return true;
};
