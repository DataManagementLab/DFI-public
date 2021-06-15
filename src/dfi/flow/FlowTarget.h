#pragma once

#include "../../utils/Config.h"
#include "../memory/local_iterators/BufferIterator.h"
#include "../memory/NodeClient.h"
#include "../registry/RegistryClient.h"
#include "../type/Schema.h"
#include "../type/Tuple.h"
#include "FlowHandle.h"

namespace dfi
{

class FlowTarget
{
public:
    //FlowTarget and NodeServer must run in the same process!
    FlowTarget(TargetID targetID, FlowHandle flowHandle);
    
    ~FlowTarget();
    
    /**
     * @brief initializeConsume pulls internal data from RegistryServer about Flow. I.e., Flow must be initialized before initializeConsume is called!
     */
    bool initializeConsume();
    
    /**
     * @brief Consumes tuples from the Flow. Function will block until either a tuple is consumable, tuple is lost or flow has finished.
     * 
     * @param returnedTuples - Count of tuples consumed, sat by FlowTarget
     * @param tuple - Tuple with ptr to memory, sat by FlowTarget
     * @param freeLastTuples - (to be impl.) If true: next consume call will free the last returned tuples. If false: tuples will not be freed (make sure Buffer is initialized with sufficient memory)
     * @return bool - True if tuple(s) were correctly consumed, (i.e. tuple ptr has been sat), false if buffer is closed
     */    
    bool consume(size_t &returnedTuples, Tuple &tuple, bool freeLastTuples = true);
    
    /**
     * @brief Consumes tuples from the Flow. Function will not block. If no tuples are consumable, returnedTuples will be 0 and true returned.
     * 
     * @param returnedTuples - Count of tuples consumed, sat by FlowTarget
     * @param tuple - Tuple with ptr to memory, sat by FlowTarget
     * @param freeLastTuples - (to be impl.) If true: next consume call will free the last returned tuples. If false: tuples will not be freed (make sure Buffer is initialized with sufficient memory)
     * @return bool - True if tuple(s) were correctly consumed, or no tuples are consumable - false if buffer is closed
     */    
    bool consumeAsync(size_t &returnedTuples, Tuple &tuple, bool freeLastTuples = true);
    
private:

    void closeFlow();
    std::unique_ptr<BufferHandle> m_bufferHandle;
    TargetID targetID;
    FlowHandle m_flowHandle;
    Schema *m_schema = nullptr;
    BufferIteratorInterface *bufferIterator = nullptr;
    bool m_flowActive = false;
};

} //dfi namespace end
