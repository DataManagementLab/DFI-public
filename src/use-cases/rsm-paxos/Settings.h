#pragma once

#include "../../utils/Config.h"
#include "utils/HashTable.h"

#define UNUSED ((uint64_t) 0)
#define HT_SIZE 4194304

#define CMD_SIZE 64
#define EMPTY_CMD {.operation = Operation::EMPTY, .key = 0, .value = 0, .padding = {0}}

#define PADDING_SIZE (CMD_SIZE - 24)
typedef uint64_t Padding[PADDING_SIZE / 8];

enum Operation {
    EMPTY = 0,
    READ = 1,
    WRITE = 2,
};

enum Mode
{
    SYNCHRONOUS = 0,
    ASYNCHRONOUS = 1,
    BATCHING = 2,
};


enum Return // of the operation
{
    SUCCESS = 0,
    FAILED = 1, // e.g. could not store value; value not found etc.. 
};

// Commands send to the RSM. We artificailly inflate the commands to emulate CMD_SIZE byte commands.
struct Command {
    uint64_t operation;
    uint64_t key;
    uint64_t value;
    Padding padding;
} __attribute__((packed));

struct ClientRequest
{
    int8_t clientid;
    Command cmd;
} __attribute__((packed));

struct ClientResponse
{
    int8_t clientid;
    int8_t ret_code;
    uint64_t ret_val;
} __attribute__((packed));

struct Phase1Request
{
    uint64_t reqid;
    uint64_t proposal_number;
    uint64_t committed;          // dummy
    uint64_t slotid;             // dummy
    Command command;            // dummy
} __attribute__((packed));


struct Phase2Request
{   
    uint64_t reqid;
    uint64_t proposal_number;
    uint64_t committed;          // up until this slotid are commands committed and can be applied
    uint64_t slotid;             // command log slot id of this command
    Command command;            // proposed command
} __attribute__((packed));

struct Phase1Response
{
    uint64_t reqid;
    int8_t ret;
    uint64_t accepted_proposal;
    Command accepted_command;
} __attribute__((packed));

struct Phase2Response
{
    uint64_t reqid; 
    int8_t ret;
    uint64_t highest_seen_proposal;
} __attribute__((packed));

struct VerificationMsg
{
    uint64_t slotid;
    Command cmd;
} __attribute__((packed));

struct Settings
{
    static string FLOWNAME_CLIENT_REQ;
    static string FLOWNAME_CLIENT_RESP;
    static string FLOWNAME_P1REQ;
    static string FLOWNAME_P2REQ;
    static string FLOWNAME_P1RESP;
    static string FLOWNAME_P2RESP;
    static string FLOWNAME_VERIFICATION;
    static NodeID LEADER_ID;
    static NodeID NODE_ID;

    static string BENCH_FILE;
    static Mode MODE;
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