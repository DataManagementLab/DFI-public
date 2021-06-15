#include "Settings.h"

string Settings::FLOWNAME_CLIENT_REQ = "CLIENT REQ FLOW";
string Settings::FLOWNAME_CLIENT_RESP = "CLIENT RESP FLOW";
string Settings::FLOWNAME_P1REQ = "PHASE 1 REQUEST FLOW";
string Settings::FLOWNAME_P2REQ = "PHASE 2 REQUEST FLOW";
string Settings::FLOWNAME_P1RESP = "PHASE 1 RESPONSE FLOW";
string Settings::FLOWNAME_P2RESP = "PHASE 2 RESPONSE FLOW";
string Settings::FLOWNAME_VERIFICATION = "LOG VERIFICATION FLOW";

string Settings::BENCH_FILE = "bench.data";
Mode Settings::MODE = Mode::ASYNCHRONOUS;
NodeID Settings::LEADER_ID = 0;
NodeID Settings::NODE_ID = 0;
uint64_t Settings::COMMAND_COUNT = 0;
uint64_t Settings::COMMANDS_PER_SECOND = 0;
size_t Settings::REPLICA_COUNT = 0;
size_t Settings::CLIENT_COUNT = 0;