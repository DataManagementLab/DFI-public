#pragma once

#include <gtest/gtest.h>
#include "../../flow-api/dfi.h"

using namespace dfi;



class TestShuffleFlow : public testing::Test {
protected:
  void SetUp() override;
  void TearDown() override;

  std::unique_ptr<RegistryServer> m_regServer;
  std::unique_ptr<DFI_Node> m_nodeServer1;
  std::unique_ptr<DFI_Node> m_nodeServer2;

  std::string flow_name = "test flow";
};