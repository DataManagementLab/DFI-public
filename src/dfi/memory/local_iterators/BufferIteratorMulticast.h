#pragma once

#include "../../../rdma-manager/src/rdma/RDMAClient.h"
#include "../../flow/FlowHandle.h"
#include "BufferIterator.h"
#include "../../../utils/Network.h"
#include "../../../utils/Config.h"

#include "../MulticastSegmentBuffer.h"
#include <queue>

namespace dfi
{


struct SegmentInfo {
    SegmentInfo() = default;

    SegmentInfo(void *addr): addr(addr) {}

    void* get_addr() {
        return reinterpret_cast<uint8_t*>(addr) + rdma::Config::RDMA_UD_OFFSET;
    }

    void* get_data() {
        return reinterpret_cast<uint8_t*>(get_addr()) + sizeof(DFI_MULTICAST_SEGMENT_HEADER_t);
    }

    seq_no_t get_seq_no() {
        return get_header()->seq_no;
    }

    DFI_MULTICAST_SEGMENT_HEADER_t* get_header() {
        return reinterpret_cast<DFI_MULTICAST_SEGMENT_HEADER_t*>(get_addr());
    }
private:
    void* addr;
public:
    SegmentInfo *next = nullptr;
};

template <bool ordering>
class SegmentManager {
    using rdmaclient_t = rdma::RDMAClient<rdma::UnreliableRDMA>;
public:
    SegmentManager(FlowHandle &flowhandle, TargetID targetId, size_t messageSizes) : flowhandle(flowhandle), num_segments(flowhandle.segmentsPerRing), segment_size(messageSizes + sizeof(DFI_MULTICAST_SEGMENT_HEADER_t)), next_segments_limit{std::min((uint16_t)(num_segments/flowhandle.sources.size()-flowhandle.sources.size()-1), (uint16_t)256)} {
        auto max_sourceid = std::max_element(flowhandle.sources.begin(), flowhandle.sources.end())->sourceId;
        size_t buffer_size = num_segments * (segment_size + rdma::Config::RDMA_UD_OFFSET);
        auto ack_messages_size = (sizeof(MulticastACK) + rdma::Config::RDMA_UD_OFFSET/*UD Header size for ack msg*/) * (max_sourceid+1);

        rdmaclient = std::make_unique<rdmaclient_t>(1.1 * (buffer_size + ack_messages_size + rdma::Config::RDMA_UD_OFFSET));
        
        rdmaclient->joinMCastGroup(flowhandle.multicastAddress, mcast_conn_id);

        // allocate buffer
        buffer = reinterpret_cast<uint8_t*>(rdmaclient->localAlloc(buffer_size - rdma::Config::RDMA_UD_OFFSET)) - rdma::Config::RDMA_UD_OFFSET;
        
        // split buffer into segments
        // register receive for segments
        // put segments into queue
        segment_infos = new SegmentInfo[num_segments];
        for (size_t i = 0; i < num_segments; i++) {
            void *addr = reinterpret_cast<uint8_t*>(buffer) + i * (segment_size + rdma::Config::RDMA_UD_OFFSET); 
            SegmentInfo *segment = new(&segment_infos[i]) SegmentInfo(addr);
            register_segment(segment);
        }

        //acknowledgements
        auto ack_msgs_ptr = reinterpret_cast<MulticastACK *>(rdmaclient->localAlloc(ack_messages_size));
        ack_message_array = MulticastACKArray(ack_msgs_ptr, ack_messages_size);

        initial_source_budget = num_segments/flowhandle.sources.size() - 2;

        sourceid_to_rdmaid.resize(max_sourceid+1);
        sources_finished.resize(max_sourceid+1);
        //Connect to all sources
        for (auto src : flowhandle.sources)
        {
            NodeID rdmaNodeID;
            rdmaclient->connect(Config::getIPFromNodeId(src.nodeId), rdmaNodeID);

            sourceid_to_rdmaid[src.sourceId] = rdmaNodeID;

            ack_message_array[src.sourceId].target_id = targetId;
            ack_message_array[src.sourceId].budget_increment = initial_source_budget;
            // std::cout << "Announce budget: " << ack_message_array[src.sourceId].budget_increment << " to " << src.sourceId << std::endl;
            //Initial ack
            DFI_MULTICAST_SEGMENT_HEADER_t hdr{static_cast<size_t>(-1), true, src.sourceId};    
            ack_message_array[hdr.source_id].seq_no = hdr.seq_no;
            rdmaclient->send(sourceid_to_rdmaid[hdr.source_id], &ack_message_array[hdr.source_id], sizeof(MulticastACK), true);
            // ack_segment(&hdr, true);
            ack_message_array[src.sourceId].budget_increment = 0; //Do increments 
        }
    }

    ~SegmentManager() {
        rdmaclient->leaveMCastGroup(mcast_conn_id);
        delete[] segment_infos;
    }

    BufferIterator::HasNextReturn can_receive_segment() {
        if (!ordering)
        {
            if (!rdmaclient->pollReceiveMCast(mcast_conn_id, false)) {
                return BufferIterator::FALSE;
            }
            auto segment = get_segment();

            ++next_segments_size;
            if (!segment->get_header()->is_last) {
        
                segment->next = next_segments;
                next_segments = segment;

                timeout_counter = 0;
                return BufferIterator::HasNextReturn::TRUE;
            }

            //is_last = true
            sources_finished[segment->get_header()->source_id] = true;
            
            free_segment(segment);
            --next_segments_size;
            Logging::debug(__FILE__, __LINE__, "BufferIterator closing for source: " + std::to_string(segment->get_header()->source_id) + " seq_no: " + to_string(segment->get_header()->seq_no));
            
            //Check if all sources finished
            if (std::all_of(flowhandle.sources.begin(), flowhandle.sources.end(), [&](const auto src) {
                return sources_finished[src.sourceId];
            })) {
                Logging::debug(__FILE__, __LINE__, "Buffer closed!");
                return BufferIterator::HasNextReturn::BUFFER_CLOSED;
            } else {
                return BufferIterator::HasNextReturn::FALSE;
            }
        }

        //Ordering:

        // check for matching out-of-order segments first
        while (next_segments != nullptr && next_segments->get_seq_no() == next_seq_no) {
            if (!next_segments->get_header()->is_last) {
                timeout_counter = 0;
                return BufferIterator::HasNextReturn::TRUE;
            }
            // std::cout << "BufferIterator closing for source: " + std::to_string(next_segments->get_header()->source_id) + '\n';
            sources_finished[next_segments->get_header()->source_id] = true;

            ++next_seq_no;
            --next_segments_size;
            std::cout << "BufferIterator closing for source: " + std::to_string(next_segments->get_header()->source_id) + '\n';
            //Check if all sources finished
            
            //Move next_segments
            auto tmp = next_segments;
            next_segments = next_segments->next;
            free_segment(tmp);

            if (std::all_of(flowhandle.sources.begin(), flowhandle.sources.end(), [&](const auto src) {
                return sources_finished[src.sourceId];
            })) {
                // std::cout << "BUFFER_CLOSED" << std::endl;
                return BufferIterator::HasNextReturn::BUFFER_CLOSED;
            } else {
                return BufferIterator::HasNextReturn::FALSE;
            }
        }
        
        if (rdmaclient->pollReceiveMCast(mcast_conn_id, false)) {
            auto segment = get_segment();

            // std::cout << "received segment " +to_string(segment->get_header()->seq_no)+ "\n";
            ++next_segments_size;
            if (segment->get_seq_no() == next_seq_no) {
                // quick_path just append to the front! Is then used directly in next()
                
                if (!segment->get_header()->is_last) {
               
                    segment->next = next_segments;
                    next_segments = segment;

                    timeout_counter = 0;
                    return BufferIterator::HasNextReturn::TRUE;
                }

                //is_last = true
                sources_finished[segment->get_header()->source_id] = true;
                
                free_segment(segment);
                ++next_seq_no;
                --next_segments_size;
                // std::cout << "BufferIterator closing for source: " + std::to_string(segment->get_header()->source_id) + " seq_no: " + to_string(segment->get_header()->seq_no) + '\n';
                //Check if all sources finished
                
                if (std::all_of(flowhandle.sources.begin(), flowhandle.sources.end(), [&](const auto src) {
                    return sources_finished[src.sourceId];
                })) {
                    // std::cout << "BUFFER_CLOSED" << std::endl;
                    return BufferIterator::HasNextReturn::BUFFER_CLOSED;
                } else {
                    return BufferIterator::HasNextReturn::FALSE;
                }
            }
            else if (segment->get_seq_no() < next_seq_no) //Check that new segment is not an old duplicate or too-far-out-of-order segment (i.e. a segment that was already considered lost)
            {
                // std::cout << "Segment (" << segment->get_seq_no() << ") was too far out of order! Next seq_no: " << next_seq_no << std::endl;
                free_segment(segment);
                --next_segments_size;

                return BufferIterator::HasNextReturn::FALSE;
            }
            
            // Segment is out of order!

            //Quick path if next_segments is empty -> append directly
            if (next_segments_end == nullptr) {
                next_segments = segment;
                next_segments_end = segment;
                segment->next = nullptr;
            }
            else if (next_segments_end->get_seq_no() < segment->get_seq_no()) //If end of next_segments is smaller, append directly to end
            {
                next_segments_end->next = segment; //Update last segment to point to segment
                next_segments_end = segment; //Update next_segments_end pointer to new last segment
                segment->next = nullptr; //segment is last, set next to nullptr
            }
            else //Scan through next_segments list and insert in order
            {
                //Segment is out of order, insert it in-place to next_segments list
                SegmentInfo **cur = &next_segments;
                while (*cur != nullptr && (*cur)->get_seq_no() < segment->get_seq_no()) {
                    cur = &(*cur)->next;
                }
                segment->next = *cur;
                *cur = segment;
            }

            if (next_segments_size >= next_segments_limit)
            {
                 // Verify that next_segments list is exactly next_segments_size long!
                Logging::warn("Replicate flow ("+flowhandle.name+"): Skipping seq_no:" + to_string(next_seq_no));
                // std::cout << next_segments_size << ' ' << next_segments_limit << '\n';
                ++next_seq_no; //skip seq_no since it is lost (or too far out of order)

                return BufferIterator::HasNextReturn::LOST;
            }
        }
        //Timeout logic
        if (next_segments_size > 0)
        {
            ++timeout_counter;
            if (timeout_counter > 10'000'000) {
                // std::cout << "Timeout skipping " << next_seq_no << std::endl;
                ++next_seq_no; //skip seq_no since it didn't arrive before timeout
                timeout_counter = 0;

                // DEBUG CODE
                // Verify that next_segments list is exactly next_segments_size long!
                SegmentInfo *tmp = next_segments;
                // std::cout << "Seq_nr ("<<next_segments_size<<"): ";
                while (tmp != nullptr) {
                    std::cout << tmp->get_seq_no() << (tmp->get_header()->is_last ? "! " : " ");
                    if (next_segments_end == tmp)
                        std::cout << "x";
                    tmp = tmp->next;
                }
                std::cout << std::endl;
            }
        }
        return BufferIterator::HasNextReturn::FALSE;
    }


    SegmentInfo* next() {
        if (next_segments == nullptr) {
            throw std::runtime_error("next_segments should never be empty");
        }
        ++next_seq_no;

        free_prev_segment();
        prev_segment = next_segments;
        next_segments = next_segments->next;
        
        if (next_segments == nullptr) { //If next_segments is the last
            next_segments_end = nullptr;
        }
        // std::cout << "(" + to_string(prev_segment->get_header()->seq_no) + ")" << std::endl;
        --next_segments_size;

        return prev_segment;
    }

    void free_prev_segment() {
        if (prev_segment != nullptr) {
            free_segment(prev_segment);
        }
    }

    SegmentInfo* peek() {
        if (next_segments == nullptr) {
            throw std::runtime_error("next_segments should never be empty");
        }
        return next_segments;
    }

    void ack_segment(const DFI_MULTICAST_SEGMENT_HEADER_t *hdr, bool signaled = true) {
        // rdmaclient->send(sourceid_to_rdmaid[hdr->source_id], &ack_message_array[hdr->source_id], sizeof(MulticastACK), signaled); //todo: Need for signaled here? -> if msg do not span cachelines it can only be updated atomically and worst case the ack contains a higher seq_no
        ++(ack_message_array[hdr->source_id].budget_increment);
        if (ack_message_array[hdr->source_id].budget_increment == (1 + initial_source_budget/4) || hdr->is_last)
        {
            ack_message_array[hdr->source_id].seq_no = hdr->seq_no;
            // if (hdr->is_last)
            // {
            //     // std::cout << "Sending last ack back! seq " + std::to_string(ack_message_array[hdr->source_id].seq_no)+'\n';
            // }
            rdmaclient->send(sourceid_to_rdmaid[hdr->source_id], &ack_message_array[hdr->source_id], sizeof(MulticastACK), signaled);
            ack_message_array[hdr->source_id].budget_increment = 0;
        }
    }

private:
    SegmentInfo* get_segment() {
        if (free_segments == nullptr) {
            throw std::runtime_error("free_segments list should never be empty");
        }
        
        SegmentInfo* segment = free_segments;
        free_segments = free_segments->next; //Move free_segments head to next segment
        segment->next = nullptr; //Decouple segment from free_segments linked list
        
        // std::cout << "free_segments=" << free_segments << '\n';
        
        return segment;
    }


    void register_segment(SegmentInfo *segment) {
        // insert at tail of free_segments list
        *free_segments_end = segment; //Update the old 'last' segment
        segment->next = nullptr;
        free_segments_end = &segment->next;

        // std::cout << "Registered receive for " << segment->get_addr() << " addr=" << segment->get_data() << '\n';
        rdmaclient->receiveMCast(mcast_conn_id, segment->get_addr(), segment_size);
    }

    void free_segment(SegmentInfo *segment) {
        register_segment(segment);
        ack_segment(segment->get_header());
    }

private:
    FlowHandle &flowhandle;
    std::unique_ptr<rdmaclient_t> rdmaclient;
    NodeID mcast_conn_id;
    size_t num_segments;
    size_t segment_size;
    size_t timeout_counter = 0;
    // segments for which an RDMA receive was registered
    // maybe replace with more efficient ring-buffer
    void *buffer;
    SegmentInfo* segment_infos = nullptr;

    SegmentInfo *free_segments = nullptr;
    SegmentInfo **free_segments_end = &free_segments;
    SegmentInfo *next_segments = nullptr;
    SegmentInfo *next_segments_end = nullptr;

    // temp buffer to free after finished next()
    SegmentInfo *prev_segment = nullptr;
    seq_no_t next_seq_no = 0;

    uint16_t next_segments_size = 0;
    const uint16_t next_segments_limit; //Must be smaller than segmentsPerRing/sources.size()

    size_t initial_source_budget = 0;

    MulticastACKArray ack_message_array;
    std::vector<NodeID> sourceid_to_rdmaid;
    std::vector<bool> sources_finished;
};

template <bool ordering>
class BufferIteratorMulticast final : public BufferIteratorInterface
{
    const FlowHandle &m_flowhandle;
    NodeID mcast_conn_id;
    
    seq_no_t last_seq_no = 0;
    bool *sources_closed;
    TargetID targetId;
    size_t segment_size;
    SegmentManager<ordering> segmentManager;

public:
    BufferIteratorMulticast(FlowHandle &flowhandle, TargetID targetId, size_t segmentSizes = 0) : m_flowhandle{flowhandle}, targetId{targetId}, segment_size{segmentSizes != 0 ? segmentSizes : flowhandle.segmentSizes}, segmentManager{flowhandle, targetId, segment_size}
    {
    }

    HasNextReturn has_next()
    {
        return segmentManager.can_receive_segment();

    }

    //Unexpected behaviour if has_next() was not previously called with TRUE return value
    void *next(size_t &ret_size)
    {
        auto next_seg = segmentManager.next();
        if (ordering)
            ret_size = segment_size;
        else
        {
            ret_size = next_seg->get_header()->data_size;
        }
        return next_seg->get_data();
    }

    void free_prev_segments(uint32_t /*num_segments*/)
    {
        //Selective freeing todo for multicast
    }

    void free_all_prev_segments()
    {
        //Selective freeing todo for multicast
    }

protected:
    void mark_prev_segment() override
    {

    }
};



} // namespace dfi

/*
EXAMPLE OF SEGMENT MANAGER

// we have two separate linked lists
freeList | | | | | | | |
now we have internally in rdma received all, our free list becomes this:
freeList |1|2|5|4|3|6|

then we pollMCast
has_next() -> call the segment manager can_receive_segments()?   yes

freeList |1|2|5|4|3|6|
can_receive_segment() -> true
freeList |2|5|4|3|6|
nextList |1|
next();
nextList ||


freeList |2|5|4|3|6|
can_receive_segment() -> true
freeList |5|4|3|6|
nextList |2|
next();
nextList ||

freeList |5|4|3|6|
can_receive_segment() -> false
freeList |4|3|6|
nextList |5|
next();     // never called

freeList |4|3|6|
can_receive_segment() -> false
freeList |3|6|
nextList |4|5|      // inserted before
next();     // never called


freeList |3|6|
can_receive_segment() -> true
freeList |6|
nextList |3|4|5|
next();    // now can return 3
nextList |4|5|

can_receive_segment() -> true
nextList |4|5|
next();
nextList |5|

.....
*/
