set(TYPE_SRC
  SmallInt.h
  SmallInt.cc
  Int.h
  Int.cc
  BigInt.h
  BigInt.cc
  UBigInt.h
  UBigInt.cc
  Floating.h
  Floating.cc
  Double.h
  Double.cc
  TinyInt.h
  TinyInt.cc
  Schema.h
  Tuple.h
  Type.h
  Type.cc
  TypeId.h
  Value.h
  Value.cc
  ) # Adding headers required for portability reasons http://voices.canonical.com/jussi.pakkanen/2013/03/26/a-list-of-common-cmake-antipatterns/
add_library(type_dfi ${TYPE_SRC})
target_include_directories(type_dfi PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(type_dfi
  )