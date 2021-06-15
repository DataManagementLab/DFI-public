/**
 * @file ReplicateFlowExamples.h
 * @author cbinnig, lthostrup, tziegler
 * @date 2018-08-17
 */

#pragma once

#include <gtest/gtest.h>

#include "../../flow-api/dfi.h"
#include "../../dfi/registry/RegistryServer.h"
#include "../../dfi/memory/NodeServer.h"

class ReplicateFlowExamples : public testing::Test 
{

public:
  size_t targets = 2;
  void SetUp() override;
  void TearDown() override;
 
protected:
  RegistryServer *m_regServer;
  vector<NodeServer*> m_nodeServers;
};