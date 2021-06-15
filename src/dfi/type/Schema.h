/**
 * @file Schema.h
 * @author cbinnig, lthostrup, tziegler
 * @date 2019-01-14
 */

#pragma once

#include "../../utils/Config.h"
#include "Type.h"

namespace dfi
{

class Schema
{

  struct Column
    {
        std::string name;
        TypeId typeId;
        uint32_t offset;
        Column(std::string &name, TypeId typeId, uint32_t offset) : name(name), typeId(typeId), offset(offset) {};
        Column(){};
    };

public:
    Schema() {}

    Schema(std::vector<std::pair<std::string, TypeId>> columns)
    {
        m_columnCount = columns.size();
        m_columns.resize(m_columnCount);
        uint32_t offset = 0;
        size_t i = 0;
        for (auto &column : columns)
        {
            auto colName = column.first;
            auto colType = column.second;
            m_columns[i] = Column(colName, colType, offset);
            offset += Type::getTypeSize(colType);
            ++i;
        }
        m_tupleSize = offset;
    }

    // Schema& operator=(const Schema& other)
    // {
    //     this->m_columns = other.m_columns;
    //     this->m_columnCount = other.m_columnCount;
    //     this->m_tupleSize = other.m_tupleSize;
    //     return *this;
    // }

    uint32_t getColumnCount() { return m_columnCount; };
    
    TypeId getTypeForColumn(uint32_t index) //0-based
    {
        return m_columns[index].typeId;
    }
    
    string getNameForColumn(uint32_t index)
    {
        return m_columns[index].name;
    }

    uint32_t getOffsetForColumn(uint32_t index) //0-based
    {
        if (index >= m_columnCount)
            throw std::runtime_error("Index out of range");
        return m_columns[index].offset;
    };


    uint32_t getOffsetForColumn(std::string const &name)
    {
        return m_columns[this->getIndexForColumn(name)].offset;
    };
    
    
    //If column not found, max uint32_t size returned
    uint32_t getIndexForColumn(std::string const &name)
    {
        for (size_t i = 0; i < m_columnCount; i++) {
            if (m_columns[i].name.compare(name) == 0) {
                return i;
            }
        }
        throw std::runtime_error("Could not find index for column name: " + name);
    };

    //Size of each tuple in bytes
    uint32_t getTupleSize()
    {
        return m_tupleSize;
    };

private:
    uint32_t m_columnCount;
    uint32_t m_tupleSize;
    std::vector<Column> m_columns; 
};
  

}