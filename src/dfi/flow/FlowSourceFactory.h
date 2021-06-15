#pragma once

#include <memory>
#include "FlowHandle.h"
#include "FlowSourceInterface.h"
#include "FlowSourceBW.h"
#include "FlowSourceBWReplicate.h"
#include "FlowSourceLat.h"
#include "FlowSourceMulticast.h"
#include "FlowSourceMulticastBW.h"

namespace dfi
{

class FlowSourceFactory {
public:
    static std::unique_ptr<FlowSourceInterface> create(FlowHandle &flowHandle, size_t sourceSegmentCount, SourceID sourceId = 0, bool nonTempSupport = false)
    {
        switch (flowHandle.optimization)
        {
        case FlowOptimization::BW:
        {
            if (flowHandle.flowtype == FlowType::REPLICATE)
            {
                return std::make_unique<FlowSourceBWReplicate>(flowHandle, sourceSegmentCount);
            }
            return std::make_unique<FlowSourceBW>(flowHandle, sourceSegmentCount, nonTempSupport);
        }
        case FlowOptimization::LAT:
            return std::make_unique<FlowSourceLat>(flowHandle);
        case FlowOptimization::MULTICAST_ORDERING:
            return std::make_unique<FlowSourceMulticast<true>>(flowHandle, sourceId);
        case FlowOptimization::MULTICAST:
            return std::make_unique<FlowSourceMulticast<false>>(flowHandle, sourceId);
        case FlowOptimization::MULTICAST_BW:
            return std::make_unique<FlowSourceMulticastBW>(flowHandle, sourceId);
        }

        throw std::invalid_argument{"Invalid flowHandle.optimization"};
    }
};

} //dfi namespace end
