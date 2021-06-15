/**
 * @file RegistryClient.h
 * @author cbinnig, lthostrup, tziegler
 * @date 2018-07-06
 */

#pragma once

#include "../../utils/Config.h"

#include "../../rdma-manager/src/proto/ProtoClient.h"
#include "../message/MessageTypes.h"
#include "../message/MessageErrors.h"
#include "../../utils/Logging.h"


#include "../memory/BufferHandle.h"
#include "../flow/FlowHandle.h"

namespace dfi
{

class RegistryClient: public rdma::ProtoClient
{

  public:
    RegistryClient();
    virtual ~RegistryClient();

    // virtual BufferHandle *createBuffer(string &name, NodeID node_id, size_t size, size_t threshold);

    /**
     * @brief Registers the buffer at the RegistryServer --> creates a mapping from name to BufferHandle
     * 
     * @param handle - BufferHandle containing meta buffer data
     * @return true - if Buffer was registered
     * @return false - if Buffer was not registered
     */
    virtual bool registerBuffer(BufferHandle *handle);

    /**
     * @brief retrieves the BufferHandle identified by name. 
     * 
     * @param name of buffer
     * @return BufferHandle* - BufferHandle will contain a full list of all entry-segments into all rings
     */
    virtual std::unique_ptr<BufferHandle> retrieveBuffer(string &buffername, string flowname = "");
    
    virtual std::unique_ptr<BufferHandle> targetJoinBuffer(string &buffername, string &flowname, TargetID targetid);

      /**
     * @brief joins a BufferHandle for appending, identified by name. 
     * 
     * @param name of buffer
     * @return BufferHandle* - BufferHandle will contain a full list of all entry-segments into all rings
     */ 
    virtual std::unique_ptr<BufferHandle> sourceJoinBuffer(std::string &buffername, string flowname = "");
    
    bool createFlow(FlowHandle &handle, bool cacheAlignSegments = false);

    bool destroyFlow(FlowHandle &handle);

    std::unique_ptr<FlowHandle> retrieveFlowHandle(string name);

    bool getFlowBarrierOffset(std::string &name, size_t &offset);
  private:
    bool appendOrRetrieveSegment(Any* sendAny);
    std::unique_ptr<BufferHandle> retrieveOrJoinBuffer(Any* sendAny,  std::string &name);
    std::string registry_ipPort;
};

} // namespace dfi