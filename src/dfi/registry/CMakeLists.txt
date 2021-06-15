
set(REGISTRY_SRC
    RegistryClient.h
    RegistryClient.cc
    RegistryServer.h
    RegistryServer.cc
  ) # Adding headers required for portability reasons http://voices.canonical.com/jussi.pakkanen/2013/03/26/a-list-of-common-cmake-antipatterns/
add_library(registry_dfi ${REGISTRY_SRC})
target_include_directories(registry_dfi PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(registry_dfi 
  utils
  rdma_lib
  dfi_message
  type_dfi
  memory_dfi
  )
