/**
 * @file Value.h
 * @author cbinnig, lthostrup, tziegler
 * @date 2019-01-14
 */

#pragma once

#include "../../utils/Config.h"
#include "../../utils/Logging.h"
#include "TypeId.h"

namespace dfi
{
class Value
{
    friend class Tuple;
public:

    // Value(const TypeId type) : m_typeId(type) {};
    Value() = default;
    Value(TypeId typeId, void* i);

    Value operator+ (const Value &rhs) const;
    Value operator- (const Value &rhs) const;
    Value operator* (const Value &rhs) const;
    Value operator/ (const Value &rhs) const;
    static Value min(const Value &lhs, const Value &rhs);
    static Value max(const Value &lhs, const Value &rhs);

    Value operator+ (const int rhs) const;
    Value operator- (const int rhs) const;
    Value operator* (const int rhs) const;
    Value operator/ (const int rhs) const;

    // Creates a dfi::Value with default value for given TypeId
    static Value getDefaultValue(TypeId typeId);

protected:
    // Value item
    union {
        int8_t boolean;
        int8_t tinyint;
        int16_t smallint;
        int32_t integer;
        int64_t bigint;
        uint64_t ubigint;
        float smalldecimal;
        double decimal;
    } m_value;
    
    // Data type
    TypeId m_typeId;
};

}
