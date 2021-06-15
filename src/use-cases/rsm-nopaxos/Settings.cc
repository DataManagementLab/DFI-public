#include "Settings.h"

string Settings::BENCH_FILE="bench.data";

string Settings::FLOWNAME_OUM = "ordered_unreliable_multicast_flow";
string Settings::FLOWNAME_RESPONSE = "client_response_flow";

string Settings::FLOWNAME_GAP_LEADER = "leader_gap_agreement_flow";
string Settings::FLOWNAME_GAP_FOLLOWER = "follower_gap_agreement_flow";

string Settings::FLOWNAME_VERIFICATION = "verification_flow";

NodeID Settings::LEADER_ID = 0;
NodeID Settings::NODE_ID = 0;
NodeID Settings::REGISTRY_ID = 0;
NodeID Settings::SEQUENCER_ID = 0;
uint64_t Settings::COMMAND_COUNT = 0;
uint64_t Settings::COMMANDS_PER_SECOND = 0;
size_t Settings::REPLICA_COUNT = 0;
size_t Settings::CLIENT_COUNT = 0;