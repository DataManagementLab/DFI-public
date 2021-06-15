#include "Value.h"

#define VALUE_OPERATION(op) \
    switch (m_typeId) \
    { \
        case TypeId::SMALLINT: { \
            auto res = m_value.smallint op rhs.m_value.smallint; \
            return Value(m_typeId, &res); } \
        case TypeId::INT: { \
            auto res = m_value.integer op rhs.m_value.integer; \
            return Value(m_typeId, &res); } \
        case TypeId::BIGINT: { \
            auto res = m_value.bigint op rhs.m_value.bigint; \
            return Value(m_typeId, &res); } \
        case TypeId::BIGUINT: { \
            auto res = m_value.ubigint op rhs.m_value.ubigint; \
            return Value(m_typeId, &res); } \
        case TypeId::FLOAT: { \
            auto res = m_value.smalldecimal op rhs.m_value.smalldecimal; \
            return Value(m_typeId, &res); } \
        case TypeId::DOUBLE: { \
            auto res = m_value.decimal op rhs.m_value.decimal; \
            return Value(m_typeId, &res); } \
        case TypeId::TINYINT: { \
            auto res = m_value.tinyint op rhs.m_value.tinyint; \
            return Value(m_typeId, &res); } \
        default: \
            break; \
    }

#define VALUE_OPERATION_LITERAL(op) \
    switch (m_typeId) \
    { \
        case TypeId::SMALLINT: { \
            auto res = m_value.smallint op rhs; \
            return Value(m_typeId, &res); } \
        case TypeId::INT: { \
            auto res = m_value.integer op rhs; \
            return Value(m_typeId, &res); } \
        case TypeId::BIGINT: { \
            auto res = m_value.bigint op rhs; \
            return Value(m_typeId, &res); } \
        case TypeId::BIGUINT: { \
            auto res = m_value.ubigint op rhs; \
            return Value(m_typeId, &res); } \
        case TypeId::FLOAT: { \
            float res = m_value.smalldecimal op (1.0*rhs); \
            return Value(m_typeId, &res); } \
        case TypeId::DOUBLE: { \
            double res = m_value.decimal op (1.0*rhs); \
            return Value(m_typeId, &res); } \
        case TypeId::TINYINT: { \
            auto res = m_value.tinyint op rhs; \
            return Value(m_typeId, &res); } \
        default: \
            break; \
    }

Value::Value(TypeId typeId, void* i) : m_typeId(typeId)
{
    switch (typeId)
    {
        case TypeId::SMALLINT:
            m_value.smallint = *(int16_t*)i;
            return;
        case TypeId::INT:
            m_value.integer = *(int32_t*)i;
            return;
        case TypeId::BIGINT:
            m_value.bigint = *(int64_t*)i;
            return;
        case TypeId::BIGUINT:
            m_value.ubigint = *(uint64_t*)i;
            return;
        case TypeId::FLOAT:
            m_value.smalldecimal = *(float*)i;
            return;
        case TypeId::DOUBLE:
            m_value.decimal = *(double*)i;
            return;
        case TypeId::TINYINT:
            m_value.tinyint = *(int8_t*)i;
            return;
        default:
            return;
    }
}

Value Value::operator+ (const Value &rhs) const
{
    if (m_typeId != rhs.m_typeId)
    {
        Logging::error(__FILE__, __LINE__, "Only arithmetic operations of same data types supported. typeid lhs: " + to_string((int)m_typeId) + " rhs: " + to_string((int)rhs.m_typeId));
        return Value();
    }

    VALUE_OPERATION(+)

    return Value();
}
Value Value::operator- (const Value &rhs) const
{
    if (m_typeId != rhs.m_typeId)
    {
        Logging::error(__FILE__, __LINE__, "Only arithmetic operations of same data types supported. typeid lhs: " + to_string((int)m_typeId) + " rhs: " + to_string((int)rhs.m_typeId));
        return Value();
    }

    VALUE_OPERATION(-)

    return Value();
}
Value Value::operator* (const Value &rhs) const
{
    if (m_typeId != rhs.m_typeId)
    {
        Logging::error(__FILE__, __LINE__, "Only arithmetic operations of same data types supported. typeid lhs: " + to_string((int)m_typeId) + " rhs: " + to_string((int)rhs.m_typeId));
        return Value();
    }

    VALUE_OPERATION(*)

    return Value();
}
Value Value::operator/ (const Value &rhs) const
{
    if (m_typeId != rhs.m_typeId)
    {
        Logging::error(__FILE__, __LINE__, "Only arithmetic operations of same data types supported. typeid lhs: " + to_string((int)m_typeId) + " rhs: " + to_string((int)rhs.m_typeId));
        return Value();
    }

    VALUE_OPERATION(/)

    return Value();
}

Value Value::min(const Value &lhs, const Value &rhs)
{
    if (lhs.m_typeId != rhs.m_typeId)
    {
        Logging::error(__FILE__, __LINE__, "Only operations on same data types supported");
        return Value();
    }

    switch (lhs.m_typeId)
    {
        case TypeId::SMALLINT: {
            auto res = std::min(lhs.m_value.smallint, rhs.m_value.smallint);
            return Value(lhs.m_typeId, &res); }
        case TypeId::INT: {
            auto res = std::min(lhs.m_value.integer, rhs.m_value.integer);
            return Value(lhs.m_typeId, &res); }
        case TypeId::BIGINT: {
            auto res = std::min(lhs.m_value.bigint, rhs.m_value.bigint);
            return Value(lhs.m_typeId, &res); }
        case TypeId::BIGUINT: {
            auto res = std::min(lhs.m_value.ubigint, rhs.m_value.ubigint);
            return Value(lhs.m_typeId, &res); }
        case TypeId::FLOAT: {
            auto res = std::min(lhs.m_value.smalldecimal, rhs.m_value.smalldecimal);
            return Value(lhs.m_typeId, &res); }
        case TypeId::DOUBLE: {
            auto res = std::min(lhs.m_value.decimal, rhs.m_value.decimal);
            return Value(lhs.m_typeId, &res); }
        case TypeId::TINYINT: {
            auto res = std::min(lhs.m_value.tinyint, rhs.m_value.tinyint);
            return Value(lhs.m_typeId, &res); }
        default:
            break;
    }
    return Value();
}

Value Value::max(const Value &lhs, const Value &rhs)
{
    switch (lhs.m_typeId)
    {
        case TypeId::SMALLINT: {
            auto res = std::max(lhs.m_value.smallint, rhs.m_value.smallint);
            return Value(lhs.m_typeId, &res); }
        case TypeId::INT: {
            auto res = std::max(lhs.m_value.integer, rhs.m_value.integer);
            return Value(lhs.m_typeId, &res); }
        case TypeId::BIGINT: {
            auto res = std::max(lhs.m_value.bigint, rhs.m_value.bigint);
            return Value(lhs.m_typeId, &res); }
        case TypeId::BIGUINT: {
            auto res = std::max(lhs.m_value.ubigint, rhs.m_value.ubigint);
            return Value(lhs.m_typeId, &res); }
        case TypeId::FLOAT: {
            auto res = std::max(lhs.m_value.smalldecimal, rhs.m_value.smalldecimal);
            return Value(lhs.m_typeId, &res); }
        case TypeId::DOUBLE: {
            auto res = std::max(lhs.m_value.decimal, rhs.m_value.decimal);
            return Value(lhs.m_typeId, &res); }
        case TypeId::TINYINT: {
            auto res = std::max(lhs.m_value.tinyint, rhs.m_value.tinyint);
            return Value(lhs.m_typeId, &res); }
        default:
            break;
    }
    return Value();
}



Value Value::operator+ (const int rhs) const
{
    VALUE_OPERATION_LITERAL(+)

    return Value();
}
Value Value::operator- (const int rhs) const
{
    VALUE_OPERATION_LITERAL(-)

    return Value();
}
Value Value::operator* (const int rhs) const
{
    VALUE_OPERATION_LITERAL(*)

    return Value();
}
Value Value::operator/ (const int rhs) const
{
    switch (m_typeId)
    {
        case TypeId::SMALLINT: {
            auto res = m_value.smallint / rhs;
            return Value(m_typeId, &res); }
        case TypeId::INT: {
            auto res = m_value.integer / rhs;
            return Value(m_typeId, &res); }
        case TypeId::BIGINT: {
            auto res = m_value.bigint / rhs;
            return Value(m_typeId, &res); }
        case TypeId::BIGUINT: {
            auto res = m_value.ubigint / rhs;
            return Value(m_typeId, &res); }
        case TypeId::FLOAT: {
            float res = m_value.smalldecimal / (1.0*rhs);
            return Value(m_typeId, &res); }
        case TypeId::DOUBLE: {
            double res = m_value.decimal / (1.0*rhs);
            return Value(m_typeId, &res); }
        case TypeId::TINYINT: {
            auto res = m_value.tinyint / rhs;
            return Value(m_typeId, &res); }
        default:
            break;
    }
    // VALUE_OPERATION_LITERAL(/)

    return Value();
}

Value Value::getDefaultValue(TypeId typeId)
{
    switch (typeId)
    {
        case TypeId::SMALLINT: {
            auto res = (int16_t)0;
            return Value(typeId, &res); }
        case TypeId::INT: {
            auto res = (int32_t)0;
            return Value(typeId, &res); }
        case TypeId::BIGINT: {
            auto res = (int64_t)0;
            return Value(typeId, &res); }
        case TypeId::BIGUINT: {
            auto res = (uint64_t)0;
            return Value(typeId, &res); }
        case TypeId::FLOAT: {
            auto res = (float)0;
            return Value(typeId, &res); }
        case TypeId::DOUBLE: {
            auto res = (double)0;
            return Value(typeId, &res); }
        case TypeId::TINYINT: {
            auto res = (int8_t)0;
            return Value(typeId, &res); }
        default:
            break;
    }
    return Value();
}