# DFI: The Data Flow Interface for High-Speed Networks
## Paper Abstract
In this paper, we propose the Data Flow Interface (DFI) as a way to
make it easier for data-processing systems to exploit high-speed
networks without the need to deal with the complexity of RDMA.
By lifting the level of abstraction, DFI factors out much of the
complexity of network communication and makes it easier for
developers to declaratively express how data should be efficiently
routed to accomplish a given distributed data processing task. As
we show in our experiment, DFI is able to support a wide variety of
data-centric applications with high performance at a low complexity
for the applications.

## Cloning repository
The DFI repository includes another repository (rdma-manager) as a submodule. Therefore, after cloning the repository run:
```
git submodule init
git submodule update
```

## Dependencies
RDMA capable NIC (tested with ConnectX-5)  
Mellanox OFED (tested with 5.1)  
Protobuf (tested with 3.10.1)  
ZeroMQ (tested with 4.2.5-1)  
GNU Make (tested with 4.1)  
CMake (tested with 3.10)  

## Examples
Examples can be found in the `src/examples/` folder.

Simple example of a shuffle flow:
```cpp
//Registry and node setup
RegistryServer registry;
DFI_Node node1(5400);
DFI_Node node2(5401);

//Flow initialization
std::string flow_name = "Shuffle-example";
size_t source_count = 1;
std::vector<target_t> targets{{1,1}, {2,2}}; // target id 1 --> node1 && target id 2 --> node 2
DFI_Schema schema({{"key", TypeId::INT},{"value", TypeId::INT}});
DFI_Shuffle_flow_init(flow_name, source_count, targets, schema, 0);

//Flow execution
DFI_Shuffle_flow_source source(flow_name);
DFI_Shuffle_flow_target target1(flow_name, 1);
DFI_Shuffle_flow_target target2(flow_name, 2);

std::pair<int, int> data;
data = {3,10};
source.push(&data);
data = {8,20};
source.push(&data);
source.close();

dfi::Tuple tuple1;
target1.consume(tuple1);

dfi::Tuple tuple2;
target2.consume(tuple2);

std::cout << "Target 1 got tuple (" << tuple1.getAs<int>("key") << "," << tuple1.getAs<int>("value") << ")" << std::endl;
std::cout << "Target 2 got tuple (" << tuple2.getAs<int>("key") << "," << tuple2.getAs<int>("value") << ")" << std::endl;
```

## DFI Use cases
Examples of distributed use cases can be found in src/use-cases/  

Use the distributed experiment runner for easily running use-cases: https://github.com/mjasny/distexprunner 

## Installation:
DFI can optionally be installed, however for development git submodule is recommended.
Install the shared library after compilation with 'make install'.
By default, files are installed at /usr/local/. This can by changed
by setting CMAKE_INSTALL_PREFIX when running cmake.

Usage:
When compiling external code that uses DFI, link it with -ldfi.
To include DFI: `#include <dfi/flow-api/dfi.h>`
