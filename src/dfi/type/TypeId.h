/**
 * @file TypeId.h
 * @author cbinnig, lthostrup, tziegler
 * @date 2019-01-14
 */

#pragma once

namespace dfi {
enum class TypeId
{
    INVALID = 0,
    SMALLINT,
    INT,
    BIGINT,
    BIGUINT,
    FLOAT,
    DOUBLE,
    TINYINT
};

}
