/**
 * @file Tuple.h
 * @author cbinnig, lthostrup, tziegler
 * @date 2019-01-14
 */

#pragma once

#include "Schema.h"
#include "Value.h"
#include <initializer_list>

namespace dfi
{

//Tuple class is a wrapper of data and schema. I.e., a Tuple object does not carry the tuple data state but only pointers to it.
//Memory management should be handled externally
class Tuple
{

public:
    Tuple(Schema *schema) : m_schema(schema) {}
    Tuple(Schema *schema, char *data) : m_schema(schema), m_data(data) {}
    Tuple(void *data) : m_data((char*)data) {}
    Tuple() : m_data(nullptr) {}

    ~Tuple()
    {
        if (freeData)
            delete[] m_data;
    }

    Tuple(const Tuple& other)
    {
        if (this != &other)
        {
            if (this->freeData == true) //if data is internally allocated in this Tuple, free memory
            {
                delete[] m_data;
            }
            this->m_schema = other.m_schema;
            if (other.freeData == true && m_schema != nullptr) //if data is internally allocated in other Tuple, copy memory
            {
                this->m_data = new char[m_schema->getTupleSize()];
                memcpy(this->m_data, other.m_data, m_schema->getTupleSize());
            }
            else
                this->m_data = other.m_data;

            this->freeData = other.freeData;
        }
    }

    Tuple& operator=(const Tuple& other)
    {
        if (this != &other)
        {
            if (this->freeData == true) //if data is internally allocated in this Tuple, free memory
            {
                delete[] m_data;
            }
            this->m_schema = other.m_schema;
            if (other.freeData == true && m_schema != nullptr) //if data is internally allocated in other Tuple, copy memory
            {
                this->m_data = new char[m_schema->getTupleSize()];
                memcpy(this->m_data, other.m_data, m_schema->getTupleSize());
            }
            else
                this->m_data = other.m_data;

            this->freeData = other.freeData;
        }
        return *this;
    }

    inline void clearDataPtr() {m_data = nullptr;}
    inline void setDataPtr(void *data) { m_data = static_cast<char*>(data); }
    inline char *getDataPtr() const { return m_data; };
    inline char *getDataPtr(const uint32_t column_id) const 
    { 
        if (m_schema != nullptr)
            return m_data + m_schema->getOffsetForColumn(column_id); 
        else
            return nullptr;
    };

    //Returns the pointer to the column specified by name of the column (for performance use column index!)
    inline char *getDataPtr(std::string const &name) const 
    { 
        if (m_schema != nullptr)
            return m_data + m_schema->getOffsetForColumn(name);
        else
            return nullptr;
    };

    template<typename T>
    T& getAs(const uint32_t column_id) const 
    { 
        return *reinterpret_cast<T*>(getDataPtr(column_id));
    };

    //Returns the pointer to the column specified by name of the column (for performance use column index!)
    template<typename T>
    T& getAs(std::string const &name) const 
    {
        return *reinterpret_cast<T*>(getDataPtr(name));
    };


    void allocateTupleMemory()
    {
        if (m_data != nullptr)
        {
            Logging::error(__FILE__, __LINE__, "Tuple's data pointer (m_data) is not default (nullptr). Overwriting might cause memory leaks! Aborting.");
            return;
        }
        if (m_schema != nullptr)
        {
            m_data = new char[m_schema->getTupleSize()];
            freeData = true;
        }
    }

    void setSchema(Schema *schema)
    {
        m_schema = schema;
    }

    inline Value getValue(const uint32_t col) const
    {
        if (m_schema != nullptr)
        {
            return Value(m_schema->getTypeForColumn(col), m_data + m_schema->getOffsetForColumn(col));
        }
        Logging::error(__FILE__, __LINE__, "Tuple was not initialized with a schema member!");
        return Value();
    }
    
    inline void setValue(const uint32_t col, const Value &val)
    {
        if (m_schema != nullptr)
        {
            memcpy(m_data + m_schema->getOffsetForColumn(col), &val.m_value, Type::getTypeSize(m_schema->getTypeForColumn(col)));
            return;
        }

        Logging::error(__FILE__, __LINE__, "Tuple was not initialized with a schema member!");
    }

private:
    Schema *m_schema = nullptr;
    char *m_data = nullptr;
    bool freeData = false;
};

}
