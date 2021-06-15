/**
 * @file PaxosClient.h
 * @author jskrzypczak
 * @date 2020-01-13
 */

#pragma once

#include <thread>
#include <iostream>
#include <fstream>
#include <vector>
#include <chrono>
#include "../../flow-api/dfi.h"
#include "Settings.h"
#include "utils/Thread.h"

#define lgc(message) std::cout << Settings::NODE_ID << " (Client) Line " << __LINE__ << ": " << (message) << std::endl

#define WRITE_PERC 5

class PaxosClient
{
private:
    std::vector<std::chrono::steady_clock::time_point> req_begins;
    int8_t clientid;
    // once the first request is done it means the system is setup properly and we can start measurements
    bool first_req_done; 

    DFI_Shuffle_flow_source flow_src;
    DFI_Shuffle_flow_target flow_target;

    void fill_request_cmd(ClientRequest&);

    class Sender : public utils::Thread
    {
        private:
            PaxosClient& client;
        public:
            Sender(PaxosClient& x);
            ~Sender();
            void run() override;
    };
    class Receiver : public utils::Thread
    {
        private:
            PaxosClient& client;
        public:
            Receiver(PaxosClient& x);
            ~Receiver();
            void run() override;
    };
public:
    PaxosClient();
    ~PaxosClient();

    void start();
};

PaxosClient::PaxosClient() :
    req_begins(Settings::COMMAND_COUNT / Settings::CLIENT_COUNT, std::chrono::steady_clock::now()),
    clientid(Settings::NODE_ID - Settings::REPLICA_COUNT), first_req_done(false),
    flow_src(Settings::FLOWNAME_CLIENT_REQ),
    flow_target(Settings::FLOWNAME_CLIENT_RESP, Settings::NODE_ID - Settings::REPLICA_COUNT)
{
}

PaxosClient::~PaxosClient()
{
}

void PaxosClient::start()
{
    utils::ThreadScheduler::start(2);

    PaxosClient::Receiver receiver(*this);
    receiver.start();
    PaxosClient::Sender sender(*this);
    sender.start();
    receiver.join();

    utils::ThreadScheduler::stop();
}

PaxosClient::Sender::Sender(PaxosClient& x) : client(x) {}
PaxosClient::Sender::~Sender(){ }

PaxosClient::Receiver::Receiver(PaxosClient& x) : client(x) { }
PaxosClient::Receiver::~Receiver() {}

void PaxosClient::Sender::run()
{
    size_t command_count = Settings::COMMAND_COUNT / Settings::CLIENT_COUNT;
    uint64_t CPS = Settings::COMMANDS_PER_SECOND / Settings::CLIENT_COUNT;
    uint64_t ns_sleep_per_cmd = 1000000000 / CPS;

    sleep(5); // enough time for all acceptors to start...

    lgc("Starting client: " + to_string(CPS) + " commands per seconds (one command per " + to_string(ns_sleep_per_cmd) + "ns)");

    for (size_t i = 0; i < command_count; ++i)
    {
        ClientRequest reqTupleRaw({
            .clientid = client.clientid,
            .cmd = EMPTY_CMD
        });
        client.fill_request_cmd(reqTupleRaw);
        dfi::Tuple reqTuple(&reqTupleRaw);

        client.req_begins[i] = std::chrono::steady_clock::now();
        client.flow_src.push(reqTuple);

        // block until new command is due
        uint64_t delta = 0;
        do {
            delta = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - client.req_begins[i]).count();
        } while (!client.first_req_done || delta < ns_sleep_per_cmd);
    }

    sleep(5);
    lgc("Closing client flow");
    client.flow_src.close();
}

void PaxosClient::Receiver::run()
{
    size_t command_count = Settings::COMMAND_COUNT / Settings::CLIENT_COUNT;

    vector<uint64_t> latencies(command_count);
    std::chrono::steady_clock::time_point total_begin = std::chrono::steady_clock::now();
    for (size_t i = 0; i < command_count; ++i)
    {
        dfi::Tuple respTuple;
        size_t tuplesConsumed;
        client.flow_target.consume(respTuple, tuplesConsumed);
        ClientResponse *resp = (ClientResponse*) respTuple.getDataPtr();
        std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();

        if (tuplesConsumed != 1)
        {
            lgc("Expected only one consumed tuple, got " + to_string(tuplesConsumed));
        }

        std::chrono::steady_clock::time_point begin = client.req_begins[i];
        uint64_t latency = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count();

        if (resp->clientid != (int8_t) client.clientid) {
            lgc("Wrong client id!");
        }

        if (!client.first_req_done)  // prevent impact of startup times on our measurements
        {
            latencies[i] = 0;
            total_begin = std::chrono::steady_clock::now();
            client.first_req_done = true;
        }
        else
        {
            latencies[i] = latency;
        }
        //lgc("Reply from " + to_string(i));
    }

    std::chrono::steady_clock::time_point total_end = std::chrono::steady_clock::now();
    uint64_t total_duration = std::chrono::duration_cast<std::chrono::milliseconds>(total_end - total_begin).count();
    uint64_t commands_per_second = command_count * 1000 / total_duration;

    lgc("CLIENT DONE");
    client.flow_src.close();

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

inline void PaxosClient::fill_request_cmd(ClientRequest& req)
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