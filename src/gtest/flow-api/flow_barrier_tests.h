#pragma once

#include <gtest/gtest.h>
#include "../../flow-api/dfi.h"

using namespace dfi;



class TestFlowBarrier : public testing::Test {
protected:
  void SetUp() override;
  void TearDown() override;

  std::unique_ptr<RegistryServer> m_regServer;
  std::unique_ptr<NodeServer> m_nodeServer;

  std::string flow_name = "test flow";
};