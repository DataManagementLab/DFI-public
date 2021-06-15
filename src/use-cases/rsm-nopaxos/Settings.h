#pragma once

#include "../../utils/Config.h"
#include "utils/HashTable.h"

#include <cstdint>

#define UNUSED ((uint64_t) 0)
#define HT_SIZE 4194304

#define CMD_SIZE 64
#define NOOP_CMD {.operation = Operation::NOOP, .key = 0, .value = 0, .padding = {0}}

#define PADDING_SIZE (CMD_SIZE - 24)
typedef uint64_t Padding[PADDING_SIZE / 8];

enum Operation {
    EMPTY = 0,
    READ = 1,
    WRITE = 2,
    NOOP = 3,
};

// Commands send to the RSM. We artificailly inflate the commands to emulate CMD_SIZE byte commands.
struct Command {
    uint64_t operation;
    uint64_t key;
    uint64_t value;
    Padding padding;
} __attribute__((packed));

enum Return
{
    SUCCESS = 0,
    FAILED = 1,
    NOOP_RET = 2
};

struct ClientRequest
{
    int8_t clientid;
    Command cmd;
    uint64_t reqid;
} __attribute__((packed));

struct ClientResponse
{
    int8_t clientid;            // used for key of shuffle flow
    uint64_t viewid;
    uint64_t log_slot_num;
    uint64_t reqid;

    int8_t leader_reply;
    int8_t return_code;
    uint64_t result;
} __attribute__((packed));

struct GapLeaderMsg
{
    int8_t replicaid;          // recipient
    uint64_t log_slot_num;
} __attribute__((packed));

struct GapFollowerMsg
{
    int8_t receiver;        // recipient
    int8_t sender;          // sender
    uint64_t log_slot_num;
    Command cmd;           // not needed for most msg, but simplifies implementation
} __attribute__((packed));

struct VerificationMsg
{
    uint64_t log_slot_num;
    Command cmd;
} __attribute__((packed));

struct Settings
{
    static string BENCH_FILE;

    static string FLOWNAME_OUM;
    static string FLOWNAME_RESPONSE;
    static string FLOWNAME_GAP_LEADER;
    static string FLOWNAME_GAP_FOLLOWER;
    static string FLOWNAME_VERIFICATION;

    static NodeID LEADER_ID;
    static NodeID REGISTRY_ID;
    static NodeID SEQUENCER_ID;
    static NodeID NODE_ID;

    static uint64_t COMMAND_COUNT; // total number of commands send during run
    static uint64_t COMMANDS_PER_SECOND; // total number of commands send during run
    static size_t REPLICA_COUNT;
    static size_t CLIENT_COUNT;
};

inline uint64_t rand_val()
{
    return (rand() % 100000000);
}

inline bool commands_equal(const Command& a, const Command& b)
{
    return a.operation == b.operation && a.key == b.key && a.value == b.value;
}