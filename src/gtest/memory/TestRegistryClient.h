/**
 * @file TestRegistryClient.h
 * @author cbinnig, lthostrup, tziegler
 * @date 2018-08-17
 */

#pragma once


#include <gtest/gtest.h>
#include "../../flow-api/dfi.h"

class TestRegistryClient : public testing::Test 
{


public:
  void SetUp() override;
  void TearDown() override;

protected:
  RegistryClient *m_regClient;
  RegistryServer *m_regServer;
  NodeServer *m_nodeServer; 
};