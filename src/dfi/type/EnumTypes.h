#pragma once

#include "../../utils/Config.h"

namespace dfi
{
    enum FlowType
    {
        SHUFFLE,
        COMBINER,
        REPLICATE
    };
    enum AggrFunc
    {
        SUM,
        SUB,
        MEAN,
        MULT,
        MAX,
        MIN,
        NONE
    };
    
    /**
     * @brief Determines when the Consumer proceeds to a new segment-ring.
     * If ASYNC: if Consumer encounter an empty segment-ring, it proceeds to next ring to find a consumable segment.
     * If SYNC: Consumer will ensure a segment is consumed before advancing to next segment-ring. I.e. a source can not have 2 segments consumed before all others have at least 1 segment consumed. 
     * 
     */
    enum ConsumeScheme
    {
        ASYNC,
        SYNC
    };
}

