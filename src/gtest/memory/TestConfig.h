/**
 * @file TestConfig.h
 * @author lthostrup
 * @date 2019-04-16
 */

#pragma once

#include <gtest/gtest.h>
#include "../../flow-api/dfi.h"

class TestConfig : public testing::Test 
{

public:
  void SetUp() override;
  void TearDown() override;

protected:
  std::string program_name = "testconfig";
};