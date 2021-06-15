/**
 * @file schema.h
 * @author cbinnig, lthostrup, tziegler
 * @date 2019-01-30
 */

#pragma once

#include "../utils/Config.h"
#include "../dfi/type/TypeId.h"
#include "../dfi/type/Schema.h"

struct DFI_Schema : public dfi::Schema
{
    /**
     * @brief Construct a new dfi schema object
     * 
     * @param columns Pairs of column names and types
     */
    DFI_Schema(std::vector<std::pair<std::string, TypeId>> columns) : dfi::Schema(columns)
    {}
};
