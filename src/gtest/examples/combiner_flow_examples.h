/**
 * @file CombinerFlowExamples.h
 * @author lthostrup
 * @date 2019-08-01
 */

#pragma once

#include <gtest/gtest.h>

#include "../../flow-api/dfi.h"
#include "../../dfi/registry/RegistryServer.h"
#include "../../dfi/memory/NodeServer.h"

class CombinerFlowExamples : public testing::Test 
{

public:
  void SetUp() override;
  void TearDown() override;
 
protected:

  std::unique_ptr<RegistryServer> m_regServer;
  std::unique_ptr<DFI_Node> m_nodeServer;
};