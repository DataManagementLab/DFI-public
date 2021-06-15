/**
 * @file RegistryServer.h
 * @author cbinnig, lthostrup, tziegler
 * @date 2018-07-06
 */

#pragma once

#include "../../utils/Config.h"

#include "../../rdma-manager/src/proto/ProtoServer.h"
#include "../../rdma-manager/src/rdma/NodeIDSequencer.h"
#include "../../rdma-manager/src/rdma/RDMAServer.h"
#include "../memory/NodeClient.h"
#include "..//message/MessageTypes.h"
#include "../message/MessageErrors.h"
#include "../memory/BufferHandle.h"
#include "../flow/FlowHandle.h"
#include "../../utils/Logging.h"
#include <mutex>

namespace dfi
{

    class RegistryServer : public rdma::RDMAServer<rdma::ReliableRDMA>
    {
        typedef rdma::RDMAServer<rdma::ReliableRDMA> RDMAServer;

    public:
        // Constructors and Destructors
        RegistryServer(size_t mem_size = Config::DFI_REGISTRY_RDMA_MEM);
        RegistryServer(RegistryServer &&) = default;
        RegistryServer(const RegistryServer &) = default;
        RegistryServer &operator=(RegistryServer &&) = default;
        RegistryServer &operator=(const RegistryServer &) = default;
        ~RegistryServer() = default;

    protected:
        /**
     * @brief handle messages 
     *  
     * @param sendMsg the incoming message
     * @param respMsg the response message
     */
        void handle(Any *sendMsg, Any *respMsg) override;

    protected:
        // Protected members
        std::map<std::string, BufferHandle> m_bufferHandles;
        std::map<std::string, FlowHandle> m_flowHandles;
        std::map<std::string, std::vector<bool>> m_activeTargets; //vector indexed by TargetID
        std::map<std::string, size_t> m_flowBarrierOffsets;

        std::unique_ptr<rdma::NodeIDSequencer> nodeIDSequencer;

    private:
        // Members
        std::unique_ptr<NodeClient> m_nodeClient;
        map<std::string, size_t> m_appendersJoinedBuffer;
        
        std::mutex m_lock;
        
        // Methods

        void handleDFICreateFlowRequest(DFICreateFlowRequest &req, DFICreateFlowResponse &resp);
        void handleDFIAppendBufferRequest(DFIAppendBufferRequest &req, DFIAppendBufferResponse &resp);
        void handleDFIRetrieveBufferRequest(DFIRetrieveBufferRequest &req, DFIRetrieveBufferResponse &resp);
        void handleDFICreateRingOnBufferRequest(DFICreateRingOnBufferRequest &req, DFICreateRingOnBufferResponse &resp);
        void handleDFIRetrieveFlowHandleRequest(DFIRetrieveFlowHandleRequest &req, DFIRetrieveFlowHandleResponse &resp);
        void handleDFIFlowBarrierRequest(DFIFlowBarrierRequest &req, DFIFlowBarrierResponse &resp);
        void handleDFIDestroyFlowRequest(DFIDestroyFlowRequest &req);

        /**
     * @brief Creates a new dfi buffer and allocates on segment on node with node_id
     * 
     * @param name of the buffer (unique)
     * @param node_id of the server where the buffer is allocated
     * @param size in bytes of the buffer
     */
        bool registerBuffer(BufferHandle *buffHandle);

        BufferHandle *createBuffer(string &name, NodeID node_id, size_t size, size_t threshold);

        BufferHandle *retrieveBuffer(string &name);

        FlowHandle *retrieveFlow(string &name);

        // CombinerFlowHandle *retrieveCombinerFlow(string& name);

        bool appendSegment(string &name, BufferSegment *segment);

        std::vector<BufferSegment> createMultipleRingsOnBuffer(BufferHandle *bufferHandle, size_t count);

        BufferSegment createRingOnBuffer(BufferHandle *bufferHandle);
    };

} // namespace dfi
