#pragma once

#include <memory>
#include <algorithm>
#include <atomic>

#include "../../utils/MPMCQueue.h"
#include "../../rdma-manager/src/rdma/RDMAServer.h"
#include "BufferWriterInterface.h"
#include "MulticastSegmentBuffer.h"

namespace dfi
{


struct AckBuffer
{
    void *buffer;
    size_t num_entries;
    size_t current_idx = 0;

    size_t size()
    {
        return num_entries * (sizeof(MulticastACK) + rdma::Config::RDMA_UD_OFFSET);
    }

    MulticastACK &get(size_t idx)
    {
        return *reinterpret_cast<MulticastACK *>(reinterpret_cast<uint8_t *>(buffer) + idx  * (sizeof(MulticastACK)+ rdma::Config::RDMA_UD_OFFSET) + rdma::Config::RDMA_UD_OFFSET);
    }

    void *get_raw(size_t idx)
    {
        return reinterpret_cast<void *>(&get(idx));
    }

    MulticastACK &next()
    {
        auto &ack = get(current_idx);
        current_idx = ((current_idx + 1) == num_entries) ? 0 : current_idx + 1;
        return ack;
    }
};

template <bool ordering>
class BufferWriterMulticast final : public BufferWriterInterface
{
    using rdmaserver_t = rdma::RDMAServer<rdma::UnreliableRDMA>; //Unreliable server
    using rdmaclient_t = rdma::RDMAClient<rdma::ReliableRDMA>; //Reliable client

    std::unique_ptr<rdmaclient_t> m_rdmaclient;
    std::unique_ptr<rdmaserver_t> m_rdmaserver;
    NodeID mcast_conn_id; // TODO proper typing: rdma::UnreliableRDMA::rdmaConnID
    const FlowHandle &flowHandle;
    SourceID sourceID;

    AckBuffer ackBuffer; //Buffer for receiving acknowledgements from targets
    MulticastSegmentBuffer segmentBuffer; //Outgoing buffer for multicast 

    // std::vector<seq_no_t> acked_seq_no;
    std::vector<seq_no_t> target_budgets;
    size_t seq_no = 0;
    NodeID m_globalSequencerID;
    seq_no_t *m_current_seq_no_ptr;
    void *current_segment;
    size_t current_segment_offset = 0;
public:
    BufferWriterMulticast(FlowHandle &flowHandle, SourceID sourceID, size_t segmentSizes = 0) : flowHandle{flowHandle}, sourceID{sourceID}, segmentBuffer{flowHandle, segmentSizes}
    {
        for (auto &target : flowHandle.targets) {
            if (target.targetId == 0 || target.targetId > flowHandle.targets.size()) {
                throw std::runtime_error("Got bad target.targetId: " + std::to_string(target.targetId));
            }
        }

        ackBuffer.num_entries = std::min(flowHandle.targets.size() * flowHandle.segmentsPerRing, (size_t)rdma::Config::RDMA_MAX_WR); // incoming ack buffer. Entries can maximum be max queue size
        // acked_seq_no.resize(std::max_element(flowHandle.targets.begin(), flowHandle.targets.end())->targetId+1);
        target_budgets.resize(std::max_element(flowHandle.targets.begin(), flowHandle.targets.end())->targetId+1);

        const auto source = std::find_if(flowHandle.sources.begin(), flowHandle.sources.end(), [&](const auto &source) {
            return source.sourceId == sourceID;
        });

        if (source == flowHandle.sources.end())
        {
            throw std::invalid_argument("sourceID " + to_string(sourceID) + " could not be found in flowHandle.sources");
        }

        const uint16_t port = Network::getPortOfConnection(Config::getIPFromNodeId(source->nodeId));
        const size_t memsize = segmentBuffer.size() + ackBuffer.size(); //Every allocate offsets with RDMA_UD_OFFSET, therefore include in memsize of server
        Logging::debug(__FILE__, __LINE__, "BufferWriterMulticast: Starting UDServer on port: " + to_string(port));
        
        if (ordering)
        {
            auto rdmaclient_size = rdma::Config::CACHELINE_SIZE;
            m_rdmaclient = std::make_unique<rdmaclient_t>(rdmaclient_size);

            m_rdmaclient->connect(Config::DFI_REGISTRY_SERVER + ":" + to_string(Config::DFI_REGISTRY_PORT), m_globalSequencerID);
            m_current_seq_no_ptr = reinterpret_cast<seq_no_t*>(m_rdmaclient->localAlloc(sizeof(seq_no_t)));
        }
        else
        {
            m_current_seq_no_ptr = new uint64_t(0);
        }
        m_rdmaserver = std::make_unique<rdmaserver_t>(flowHandle.name + to_string(sourceID), port, memsize);
        ackBuffer.buffer = reinterpret_cast<uint8_t*>(m_rdmaserver->localAlloc(ackBuffer.size() - rdma::Config::RDMA_UD_OFFSET/*localAlloc adds rdma ud header size*/)) - rdma::Config::RDMA_UD_OFFSET;
        segmentBuffer.buffer = reinterpret_cast<uint8_t*>(m_rdmaserver->localAlloc(segmentBuffer.size() - rdma::Config::RDMA_UD_OFFSET/*localAlloc adds rdma ud header size*/)) - rdma::Config::RDMA_UD_OFFSET;

        Logging::debug(__FILE__, __LINE__, "BufferWriterMulticast: Joining Multicast flow address: " + flowHandle.multicastAddress);
        m_rdmaserver->joinMCastGroup(flowHandle.multicastAddress, mcast_conn_id);
        
        for (size_t i = 0; i < ackBuffer.num_entries; ++i)
        {
            // std::cout << "Addr: " << ackBuffer.get_raw(i) << " Byte diff ackBuffer: " << reinterpret_cast<uint8_t*>(ackBuffer.get_raw(i+1)) - reinterpret_cast<uint8_t*>(ackBuffer.get_raw(i)) << std::endl;
            m_rdmaserver->receive(0 /*dummy id*/, ackBuffer.get_raw(i), sizeof(MulticastACK));
        }

        m_rdmaserver->startServer(); //Start server AFTER registering receives! (such that no ack's are missed)

        // if (!m_sequenceNumberFetcher->running())
        //     m_sequenceNumberFetcher->start();
        
        current_segment = segmentBuffer.next();
    }

    ~BufferWriterMulticast()
    {
        m_rdmaserver->leaveMCastGroup(mcast_conn_id);
    }

    bool add(const void *data, size_t size, bool) override
    {
        std::memcpy(reinterpret_cast<uint8_t*>(current_segment) + sizeof(DFI_MULTICAST_SEGMENT_HEADER_t) + current_segment_offset, data, size);
        current_segment_offset += size;
        assert(current_segment_offset <= segmentBuffer.segment_size - sizeof(DFI_MULTICAST_SEGMENT_HEADER_t));
        return true;
    }


    bool close(bool signaled = true) override
    {  
        if (current_segment_offset > 0)
            flush();

        if (ordering)
        {
            m_rdmaclient->fetchAndAdd(m_globalSequencerID, flowHandle.globalSeqNoOffset, m_current_seq_no_ptr, sizeof(seq_no_t), true);
        }
        else
        {
            *m_current_seq_no_ptr = 0;
        }
        do
        {
            checkReceiveAck();
        } while (!can_send());
        
        void *segment = segmentBuffer.next(*m_current_seq_no_ptr, true, sourceID);

        m_rdmaserver->sendMCast(mcast_conn_id, segment, segmentBuffer.segment_size, signaled);
        // std::cout << to_string(sourceID) + " >>>> X - CLOSING seq " + to_string(new_seq_no) + '\n';
        for (auto &budget : target_budgets)
        {   
            --budget;
        }

        return true;
    }

    inline bool flush(bool forceSignaled = false, bool isEndSegment = false) override
    {
        (void)isEndSegment;
        do
        {
            checkReceiveAck();
        } while (!can_send());
        if (ordering)
        {
            m_rdmaclient->fetchAndAdd(m_globalSequencerID, flowHandle.globalSeqNoOffset, m_current_seq_no_ptr, sizeof(seq_no_t), true);
        }
        //set header
        if (ordering)
            new (current_segment) DFI_MULTICAST_SEGMENT_HEADER_t{*m_current_seq_no_ptr, false, sourceID};
        else
            new (current_segment) DFI_MULTICAST_SEGMENT_HEADER_t{current_segment_offset, false, sourceID}; //to support batching --> set size of data in header
        // std::cout << "Sending! size: " << segmentBuffer.segment_size << " real: " << current_segment_offset << std::endl;

        m_rdmaserver->sendMCast(mcast_conn_id, current_segment, segmentBuffer.segment_size, forceSignaled);

        for (auto &budget : target_budgets)
        {   
            --budget;
        }
        current_segment = segmentBuffer.next();
        current_segment_offset = 0;
        return true;
    }

private:

    // std::vector<size_t> pushed_seq_nos;
    // std::vector<std::pair<size_t, size_t>> acked_seq_nos;

    void checkReceiveAck()
    {
        while (m_rdmaserver->pollReceive(0 /* dummy id*/, false) > 0)
        { // try to receive all pending segments in rq
            auto &ack = ackBuffer.next();
            // acked_seq_no[ack.target_id] = ack.seq_no;
            target_budgets[ack.target_id] += ack.budget_increment;
            // std::cout << to_string(sourceID) + " <<<< " + to_string(ack.target_id) + " New budget " + to_string(target_budgets[ack.target_id]) + " - " + to_string(ack.seq_no) + '\n';
            // std::cout << to_string(sourceID) + " <<<< " + to_string(ack.target_id) + " Recv ack " + to_string(ack.seq_no) + '\n';
            m_rdmaserver->receive(mcast_conn_id, reinterpret_cast<void *>(&ack), sizeof(MulticastACK));
            // acked_seq_nos.push_back({ack.seq_no, ack.budget_increment});

        }
    }

    bool can_send()
    {
        return std::all_of(target_budgets.begin()+1, target_budgets.end(), [&](const auto budget) {
            // std::cout << "diff=" << (*m_current_seq_no_ptr - acked_seq_no) << " rhs=" << (flowHandle.segmentsPerRing - flowHandle.targets.size()) / flowHandle.targets.size() << '\n';
            // return (*m_current_seq_no_ptr - acked_seq_no) < (flowHandle.segmentsPerRing - flowHandle.sources.size()) / flowHandle.sources.size();
            return budget > 0;
         });
    }

    bool all_sent()
    {
        // return std::all_of(acked_seq_no.begin()+1, acked_seq_no.end(), [&](const auto acked_seq_no) {
        //     return *m_current_seq_no_ptr == acked_seq_no;
        // });
        return true;
    }
};



} // namespace dfi