#include <unistd.h>
#include <stdio.h>
#include <getopt.h>
#include "../../utils/Config.h"
#include "Settings.h"
#include "DistributedRadixJoin.h"
#include "utils/ThreadScheduler.h"

void usage(char* argv[]) {
	cout << "Usage: " << argv[0] << " nodeid total_nodes thread_count full_left_relation_size full_right_relation_size random_order(t/f) numa_node(0/1)" << endl;
}

int main(int argc, char *argv[])
{
	if (argc <= 6) {
		usage(argv);
		return -1;
	}

    rdma::Config rdmaConf(argv[0]);
    dfi::Config conf(argv[0]);

    Settings::NODE_ID = atoi(argv[1]);
    Settings::NODES_TOTAL = atoi(argv[2]);
    Settings::THREADS_COUNT = atoi(argv[3]);
    Settings::LEFT_REL_FULL_SIZE = strtoul(argv[4], NULL, 0);
    Settings::RIGHT_REL_FULL_SIZE = strtoul(argv[5], NULL, 0);

	if (argc >= 7) {
        std::string random_order(argv[6]);
        Settings::RANDOM_ORDER = (random_order == "t");
        if (Settings::RANDOM_ORDER) std::cout << "Random tuple order!" << '\n';
	}
	if (argc >= 8) {
        rdma::Config::RDMA_NUMAREGION = atoi(argv[7]);
        rdma::Config::RDMA_INTERFACE = "ib"+to_string(rdma::Config::RDMA_NUMAREGION);
	}

    if (dfi::Config::DFI_NODES.size() != Settings::NODES_TOTAL)
    {
        std::cout << "Config was initialized with " << dfi::Config::DFI_NODES.size() << " nodes. Does not match passed number of nodes: " << Settings::NODES_TOTAL << std::endl;
        exit(1);
    }

    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(rdma::Config::NUMA_THREAD_CPUS[rdma::Config::RDMA_NUMAREGION][0], &cpuset);
    sched_setaffinity(0, sizeof(cpuset), &cpuset);
    
    DistributedRadixJoin join;

    utils::ThreadScheduler::start(Settings::THREADS_COUNT);

    std::cout << "Generating test data... full left: " << Settings::LEFT_REL_FULL_SIZE << " full right: " << Settings::RIGHT_REL_FULL_SIZE << '\n';
    join.generate_test_data();

    join.initialize_dfi();

    join.run();

    utils::ThreadScheduler::stop();
}
