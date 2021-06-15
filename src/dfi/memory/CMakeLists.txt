
set(DFI_SRC
    NodeClient.h
    NodeClient.cc
    NodeServer.h
    NodeServer.cc
    BufferWriter.h
    BufferWriter.cc
    BufferWriterLocal.h
    BufferHandle.h
    BufferWriterMulticast.h
  ) # Adding headers required for portability reasons http://voices.canonical.com/jussi.pakkanen/2013/03/26/a-list-of-common-cmake-antipatterns/
add_library(memory_dfi ${DFI_SRC})
target_include_directories(memory_dfi PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(memory_dfi 
  registry_dfi
  rdma_lib
  dfi_message
  utils
  )
