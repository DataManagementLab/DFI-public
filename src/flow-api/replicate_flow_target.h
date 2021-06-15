/**
 * @file shuffle_flow_target.h
 * @author cbinnig, lthostrup, tziegler
 * @date 2019-01-30
 */

#pragma once

#include "../utils/Config.h"
#include "../dfi/flow/FlowTarget.h"
#include "../dfi/registry/RegistryClient.h"
#include "../dfi/type/ErrorCodes.h"

class DFI_Replicate_flow_target
{
public:
    
    /**
     * @brief Construct a new dfi replicate flow target object
     * 
     * @param flowName Unique flow name identifier
     * @param targetId Target identifier - Must correspond to TargetID passed to flow initialization
     */
    DFI_Replicate_flow_target(string flowName, TargetID targetId)
    {
        dfi::RegistryClient regClient;
        m_flowHandle = regClient.retrieveFlowHandle(flowName);
        while(m_flowHandle == nullptr)
        {
            m_flowHandle = regClient.retrieveFlowHandle(flowName);
            usleep(Config::DFI_SLEEP_INTERVAL);
        }
        
        m_flowTarget = std::make_unique<dfi::FlowTarget>(targetId, *m_flowHandle);
        while(!m_flowTarget->initializeConsume())
        { 
            usleep(Config::DFI_SLEEP_INTERVAL);
        }
    }

    ~DFI_Replicate_flow_target() = default;


    /**
     * @brief Consumes (potentially multiple) tuples out of the shuffle flow. Function blocks until either a tuple is consumable or flow has ended
     * 
     * @param tuple Consumed tuple(s). If multiple tuples consumed, they can be accessed by moving data pointer of tuples since they are contiguous in memory
     * @param tuplesConsumed Number of tuples that has been consumed in function call
     * @return DFI return code
     */
    inline int consume(dfi::Tuple &tuple, size_t &tuplesConsumed)
    {
        if (m_flowTarget->consume(tuplesConsumed, tuple))
        {
            if (tuplesConsumed == 0) //flowtarget consume will return true but 0 tuplesConsumed in case a message is lost over unreliable connection!
                return DFI_MESSAGE_LOST;
            return DFI_SUCCESS;
        }
        return DFI_FLOW_FINISHED;
    }
    
    /**
     * @brief Consume a single tuple out of the shuffle flow. Function blocks until either a tuple is consumable or flow has ended
     * 
     * @param tuple Consumed tuple
     * @return DFI return code
     */
    inline int consume(dfi::Tuple &tuple)
    {
        if (tuple_buffer_count == 0)
        {
            auto ret = this->consume(tuple, tuple_buffer_count);
            if (ret == DFI_SUCCESS)
            {
                tuple_buffer_ptr = tuple.getDataPtr();
            }
            else
            {
                return ret;
            }
        }

        tuple.setDataPtr(tuple_buffer_ptr);
        tuple_buffer_ptr += m_flowHandle->schema.getTupleSize();
        --tuple_buffer_count;
        return DFI_SUCCESS;
    }

    /**
     * 
     * @brief Get the DFI schema
     * 
     * @param schema reference sat by function
     * @return DFI return code
     */
    int get_schema(dfi::Schema &schema)
    {
        schema = m_flowHandle->schema;
        return DFI_SUCCESS;
    }

    /**
     * @brief Create a DFI tuple instantiated with schema
     * 
     * @return dfi::Tuple 
     */
    dfi::Tuple create_tuple()
    {
        return dfi::Tuple(&m_flowHandle->schema);
    }
    

private:
    std::unique_ptr<dfi::FlowTarget> m_flowTarget;
    std::unique_ptr<dfi::FlowHandle> m_flowHandle;
    size_t tuple_buffer_count = 0;
    char *tuple_buffer_ptr = nullptr;
};
