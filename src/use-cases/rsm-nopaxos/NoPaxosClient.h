/**
 * @file NoPaxosClient.h
 * @author jskrzypczak
 * @date 2020-02-14
 */

#pragma once

#include <thread>
#include <iostream>
#include <fstream>
#include <vector>
#include <chrono>
#include "../../flow-api/dfi.h"
#include "KV_SM.h"
#include "Settings.h"
#include "utils/Thread.h"

#define lgc(message) std::cout << Settings::NODE_ID << " (Client) Line " << __LINE__ << ": " << (message) << std::endl

#define WRITE_PERC 5

struct req_info
{
    uint64_t reqid;
    uint64_t viewid;
    uint64_t log_slot;
    bool leader_replied;
    uint8_t ack_count;
    uint64_t result; // unused so far
};

class NoPaxosClient
{
    class Sender : public utils::Thread
    {
        private:
            NoPaxosClient& client;
        public:
            Sender(NoPaxosClient& x);
            ~Sender();
            void run() override;
    };
    class Receiver : public utils::Thread
    {
        private:
            NoPaxosClient& client;
        public:
            Receiver(NoPaxosClient& x);
            ~Receiver();
            void run() override;
    };

private:
    std::vector<std::chrono::steady_clock::time_point> req_begins;
    int8_t clientid;
    // once the first request is done it means the system is setup properly and we can start measurements
    bool first_req_done; 

    DFI_Replicate_flow_source request_flow;
    DFI_Shuffle_flow_target response_flow;

    NoPaxosClient::Sender sender;
    NoPaxosClient::Receiver receiver;

    std::vector<req_info> open_reqs;
    size_t open_reqs_size;

    bool add_reply(ClientResponse*);
    bool got_quorum(const req_info&);
    void fill_request_cmd(ClientRequest&);
public:
    NoPaxosClient();
    ~NoPaxosClient();

    void start();
    void join();
};

NoPaxosClient::NoPaxosClient() :
    req_begins(Settings::COMMAND_COUNT / Settings::CLIENT_COUNT, std::chrono::steady_clock::now()),
    clientid(Settings::NODE_ID - Settings::REPLICA_COUNT), first_req_done(false),
    request_flow(Settings::FLOWNAME_OUM, clientid),
    response_flow(Settings::FLOWNAME_RESPONSE, clientid),
    sender(*this), receiver(*this),
    open_reqs(Settings::COMMAND_COUNT), open_reqs_size(Settings::COMMAND_COUNT)
{
}

NoPaxosClient::~NoPaxosClient()
{
}

void NoPaxosClient::start()
{
    receiver.start();
    sender.start();
}

void NoPaxosClient::join()
{
    receiver.join();
}

NoPaxosClient::Sender::Sender(NoPaxosClient& x) : client(x) {}
NoPaxosClient::Sender::~Sender(){ }

NoPaxosClient::Receiver::Receiver(NoPaxosClient& x) : client(x) { }
NoPaxosClient::Receiver::~Receiver() {}

void NoPaxosClient::Sender::run()
{
    size_t command_count = Settings::COMMAND_COUNT / Settings::CLIENT_COUNT;
    uint64_t CPS = Settings::COMMANDS_PER_SECOND / Settings::CLIENT_COUNT;
    uint64_t ns_sleep_per_cmd = 1000000000 / CPS;

    sleep(5); // enough time for all acceptors to start...

    lgc("Starting client (cid " + to_string(client.clientid) + "): " + to_string(CPS) +
        " commands per seconds (one command per " + to_string(ns_sleep_per_cmd) + "ns)");

    for (size_t i = 0; i < command_count; ++i)
    {
        uint64_t reqid = i;
        ClientRequest reqTupleRaw(
            {.clientid = (int8_t) client.clientid,
            .cmd = {
                .operation = 0,
                .key = 0,
                .value = 0,
                .padding = {0}
            },
            .reqid = reqid});
        client.fill_request_cmd(reqTupleRaw);

        dfi::Tuple reqTuple(&reqTupleRaw);

        client.req_begins[i] = std::chrono::steady_clock::now();
        client.open_reqs[i % client.open_reqs_size] = {reqid, 0, 0, false, 0, 0};

        client.request_flow.push(reqTuple);
        // block until new command is due
        uint64_t delta = 0;
        do {
            delta = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - client.req_begins[i]).count();
        } while (!client.first_req_done || delta < ns_sleep_per_cmd);
    }

    sleep(5);
    lgc("Closing client flow");
    client.request_flow.close();
}

void NoPaxosClient::Receiver::run()
{
    size_t command_count = Settings::COMMAND_COUNT / Settings::CLIENT_COUNT;
    vector<uint64_t> latencies(command_count);
    std::chrono::steady_clock::time_point total_begin = std::chrono::steady_clock::now();

    dfi::Tuple tuple;
    size_t tuplesConsumed;
    while (client.response_flow.consume(tuple, tuplesConsumed) != DFI_FLOW_FINISHED)
    {
        if (tuplesConsumed != 1)
        {
            lgc("Expected only one consumed tuple, got " + to_string(tuplesConsumed));
        }

        ClientResponse* resp = (ClientResponse*) tuple.getDataPtr();

        if (resp->clientid != (int8_t) client.clientid) {
            lgc("Wrong client id! We are: " + to_string(resp->clientid) + "; Got reply for: " + to_string(client.clientid)) ;
        }

        if (client.add_reply(resp)) {
            // got a quorum!
            //lgc("Got a quorum for request " + to_string(resp->reqid));

            uint64_t idx = resp->reqid;
            std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
            std::chrono::steady_clock::time_point begin = client.req_begins[idx];
            uint64_t latency = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count();

            if (!client.first_req_done)  // prevent impact of startup times on our measurements
            {
                latencies[idx] = 0;
                total_begin = std::chrono::steady_clock::now();
                // the sender waits until this flag is set to true, thus no other requests are
                // currently in progress
                client.first_req_done = true; 
            }
            else
            {
                latencies[idx] = latency;
            }
            if (idx == command_count - 1) {
                // we are done as commands are processed in-order
                break;
            }
        }
    }

    //for (auto const& i: latencies )
    //{
    //    lgc(to_string(i));
    //}

    std::chrono::steady_clock::time_point total_end = std::chrono::steady_clock::now();
    uint64_t total_duration = std::chrono::duration_cast<std::chrono::milliseconds>(total_end - total_begin).count();
    uint64_t commands_per_second = command_count * 1000 / total_duration;

    lgc("CLIENT DONE");

    // benchmark is done. writing results
    ofstream benchfile;
    benchfile.open(Settings::BENCH_FILE);
    for (auto lat : latencies)
    {
        benchfile << lat << "\n";
    }
    benchfile << "throughput " << commands_per_second << "\n";
    benchfile.close();

    // aggragates for log overview
    std::sort(latencies.begin(), latencies.end()); 
    uint64_t avg = 0;
    for (auto lat : latencies)
    {
        avg += lat;
    }
    avg /= command_count;

    std::cout << "Average latency " << avg << endl;
    std::cout << "Median latency " << latencies[command_count * 0.5] << endl;
    std::cout << "75th pctl latency " << latencies[command_count * 0.75] << endl;
    std::cout << "90th pctl latency " << latencies[command_count * 0.90] << endl;
    std::cout << "95th pctl latency " << latencies[command_count * 0.95] << endl;
    std::cout << "98th pctl latency " << latencies[command_count * 0.98] << endl;
    std::cout << "99th pctl latency " << latencies[command_count * 0.99] << endl;
    std::cout << "99.9th pctl latency " << latencies[command_count * 0.999] << endl;
    std::cout << "Achieved Throughput " << commands_per_second << endl;

    sleep(5); // hacky to ensure that log verification is done
}

inline bool NoPaxosClient::add_reply(ClientResponse* response)
{
    req_info& request = open_reqs[response->reqid % open_reqs.size()];
    if (request.reqid != response->reqid)
    {
        // outdated reply... do nothing
        return false;
    }

    if (request.ack_count == 0)
    {
        request.viewid = response->viewid;
        request.log_slot = response->log_slot_num;
        request.ack_count = 1;
    }
    else if (request.log_slot == response->log_slot_num && request.viewid == response->viewid)
    {
        request.ack_count++;
    }
    else
    {
        // these checks are sepcified in the protocol, but does this ever happen?
        lgc("Inconsistent log slot numbers (reqid " + to_string(request.reqid) + ") ! Expected " + to_string(request.log_slot) + " got " + to_string(response->log_slot_num));
    }

    if (response->leader_reply) {
        request.leader_replied = true;
        if (response->return_code == Return::NOOP_RET)
        {
            lgc("Got a noop operation! We should retry the request...");
        }
    }

    return got_quorum(request);
}

inline bool NoPaxosClient::got_quorum(const req_info& req)
{
    return req.leader_replied && req.ack_count >= 1 + (Settings::REPLICA_COUNT / 2);
}

inline void NoPaxosClient::fill_request_cmd(ClientRequest& req)
{
    if ((rand() % 100) + 1 > WRITE_PERC)
    {
        // generate read
        req.cmd.operation = Operation::READ;
        req.cmd.key = rand_val();
    }
    else
    {
        // generate write
        req.cmd.operation = Operation::WRITE;
        req.cmd.key = rand_val();
        req.cmd.value = rand_val();
    }
    
}