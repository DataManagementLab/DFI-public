

#ifndef DFI_MESSAGEERRORS_H_
#define DFI_MESSAGEERRORS_H_

#include "../../utils/Config.h"

namespace dfi {

enum MessageErrors
  : unsigned int {

  NO_ERROR = 0,
  INVALID_MESSAGE,  // make use of auto-increment

  DFI_CREATE_RING_ON_BUF_FAILED = 500,
  DFI_RTRV_BUFFHANDLE_FAILED = 501,
  DFI_APPEND_BUFFHANDLE_FAILED = 502,
  DFI_REGISTER_BUFFHANDLE_FAILED = 503,
  DFI_JOIN_BUFFER_FAILED = 504,
  DFI_NO_BUFFER_FOUND = 505,
  DFI_ALLOCATE_SEGMENT_FAILED = 506,

  DFI_CREATE_FLOW_FAILED = 600,
  DFI_RTRV_FLOWHANDLE_FAILED = 601,
  DFI_NO_FLOW_FOUND = 602,
  DFI_CREATE_COMBINER_FLOW_FAILED = 603,
  DFI_INSUFFICIENT_MEM = 604,
};

}

#endif /* DFI_MESSAGEERRORS_H_ */