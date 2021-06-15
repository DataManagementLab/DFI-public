/**
 * @file flow_barrier_init.h
 * @author lthostrup
 * @date 2020-03-01
 */

#pragma once

#include "../utils/Config.h"
#include "schema.h"
#include "../dfi/registry/RegistryClient.h"
#include "../dfi/type/ErrorCodes.h"

/**
 * @brief DFI_Flow_barrier_init initializes a barrier for a pre-initialized flow. 
 * 
 * @param name Unique flow name identifier
 * @return int - Return status
 */
inline int DFI_Flow_barrier_init(std::string &name)
{
    dfi::RegistryClient regClient;
    size_t offset;
    if (!regClient.getFlowBarrierOffset(name, offset))
        return DFI_FAILURE;
    return DFI_SUCCESS;
};
