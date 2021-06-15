#pragma once

#include "../../utils/Config.h"
#include "FlowHandle.h"
#include "../type/Schema.h"
#include "../type/Value.h"
#include "../type/Tuple.h"
#include <algorithm>
#include <sys/time.h>
#include "../registry/RegistryClient.h"
#include "../memory/NodeClient.h"

//Setting this flag will optimize the inject method to do non-temporal stores of cache-line size.
//Better performance is only achieved if fan-out is big (i.e., many partitions), and if tuples are small (more tuples fit into cacheline)
//WARNING: - only enable if the cacheline size is divisible by the size of a tuple
//         - sendTuplesCount * size of tuple is divisible by size of cacheline.
//         - AVX needs to be enabled
        // - FLOWSOURCE_CACHE_LINE_SIZE must be sat!
// #define FLOWSOURCE_NONTEMP_CACHELINE_STORES_OPTIMIZATION
#define FLOWSOURCE_CACHE_LINE_SIZE 64


#ifdef FLOWSOURCE_NONTEMP_CACHELINE_STORES_OPTIMIZATION
    #include <immintrin.h>
#endif

namespace dfi
{


class FlowSourceInterface {
protected:
    FlowHandle m_flowHandle;
    RegistryClient m_registryClient;
    const size_t m_tupleSize = 0;
    bool m_isClosed = false;
public:
        FlowSourceInterface(FlowHandle flowHandle) : m_flowHandle{flowHandle}, m_tupleSize{m_flowHandle.schema.getTupleSize()}
        {
            if (m_flowHandle.targets.empty()) {
                throw invalid_argument("Passed flow-handle does not contain any targets!");
            }

            if (m_tupleSize == 0) {
                throw invalid_argument("Passed flow-handle has invalid tuple size of 0!");
            }

            Logging::debug(__FILE__, __LINE__, "Creating FlowSource, target size: " + to_string(m_flowHandle.targets.size()));            
        }

        virtual ~FlowSourceInterface() = default;
        
        virtual bool push(Tuple const &tuple, TargetID targetId, bool forceSignaled=false) = 0;

        virtual bool pushBulk(void *, TargetID, size_t, bool)
        {
            throw new runtime_error("pushBulk functionality is not supported for specified Optimization");
        }
        virtual bool pushCacheLineNonTemp(void *, TargetID, bool)
        {
            throw new runtime_error("pushBulk functionality is not supported for specified Optimization");
        }

        virtual bool pushToAll(Tuple const &tuple, bool forceSignaled=false) {
            bool success = true;
            for (const auto &target : m_flowHandle.targets)
            {
                success &= push(tuple, target.targetId, forceSignaled);
            }
            return success;
        }
        virtual void close() = 0;

        virtual bool flush(TargetID targetId) = 0;

        virtual bool flushToAll() {
            bool success = true;
            for (const auto &target : m_flowHandle.targets)
            {
                success &= flush(target.targetId);
            }
            return success;
        }
};




} //dfi namespace end