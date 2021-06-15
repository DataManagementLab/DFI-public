/**
 * @file ShuffleFlowExamples.h
 * @author cbinnig, lthostrup, tziegler
 * @date 2018-08-17
 */

#pragma once

#include "../../flow-api/dfi.h"
#include <gtest/gtest.h>


class ShuffleFlowExamples : public testing::Test 
{

public:
  void SetUp() override;
  void TearDown() override;
};