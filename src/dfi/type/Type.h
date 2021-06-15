/**
 * @file Type.h
 * @author cbinnig, lthostrup, tziegler
 * @date 2019-01-14
 */

#pragma once

#include "../../utils/Config.h"
#include "TypeId.h"

namespace dfi
{
class Type
{
public:

    Type(TypeId typeId) : m_typeId(typeId) {}

    virtual ~Type() {}

    static uint64_t getTypeSize(TypeId typeId);

    inline TypeId getTypeId() const { return m_typeId; }

    inline static Type* getInstance(TypeId typeId) { return m_instTypes[static_cast<int>(typeId)]; }

protected:
    TypeId m_typeId;

    // Singleton instances of the types
    static Type* m_instTypes[]; //array size should match size of TypeId enum!
};

}
