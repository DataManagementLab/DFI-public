from distexprunner import *
import datetime
import math
'''
run_exp -m "dfi testing, not critical" -- python3.7 ~/distexprunner/server.py -vv -rf
python3.7 ~/distexprunner/client.py -vv --log output.log --progress 01_test.py
'''

server_list = ServerList(
    Server('node01', 'c01.lab', port=20001, ib_interface='172.18.94.1x'),
    Server('node02', 'c02.lab', port=20001, ib_interface='172.18.94.2x'),
    Server('node03', 'c03.lab', port=20001, ib_interface='172.18.94.3x'),
    Server('node04', 'c04.lab', port=20001, ib_interface='172.18.94.4x'),
    # Server('node05', 'c05.lab', port=20001, ib_interface='172.18.94.5x'),
    working_directory = "/home/lthostrup/dfi_library_private"
)

NUMA_NODES = [1]

def set_dfi_conf(servers, node_count):
    ips = []
    for i in range(node_count):
        server = servers[i%len(servers)]
        node = i%len(servers) + 1
        numa_region = NUMA_NODES[int(i/len(servers))]
        ips.append(server.ib_interface.replace('x',str(numa_region)))

    print(ips)
    ips_cmd = f"sed -i -E 's/^DFI_NODES.*=.+/DFI_NODES = {','.join(map(lambda x: f'{x}:7400', ips))}/g' src/conf/DFI.conf"
    procs = [s.run_cmd(ips_cmd) for s in servers]
    assert(all(p.wait() == 0 for p in procs))

    reg_ip_cmd = f"sed -i -E 's/^DFI_REGISTRY_SERVER.*=.+/DFI_REGISTRY_SERVER = {ips[0]}/g' src/conf/DFI.conf"
    procs = [s.run_cmd(reg_ip_cmd) for s in servers]
    assert(all(p.wait() == 0 for p in procs))


def compile(servers, left_size):
    bits = math.floor(math.log2((8*left_size)/(16*1024))) #Number of bits needed to reach L1 cache size
    
    if bits > 11:
        bits = 11

    partition_bits_cmd = f"sed -i -E 's/_PARTITION_BITS = [0-9]+;/_PARTITION_BITS = {bits};/g' src/use-cases/distributed-replicate-join-flow/Settings.h"
    procs = [s.run_cmd(partition_bits_cmd) for s in servers]
    assert(all(p.wait() == 0 for p in procs))
    log(f'Compiling with {bits} bits for local partition pass')

    cmake_cmd = f'mkdir -p build && cd build && cmake -DCMAKE_BUILD_TYPE=Release -D CMAKE_C_COMPILER=gcc-10 -D CMAKE_CXX_COMPILER=g++-10 ..'
    procs = [s.run_cmd(cmake_cmd) for s in servers]
    assert(all(p.wait() == 0 for p in procs))

    make_cmd = f'cd build && make distributed_replicate_join_flow -j'
    procs = [s.run_cmd(make_cmd) for s in servers]
    assert(all(p.wait() == 0 for p in procs))


parameter_grid = ParameterGrid(
    ratio=[10], # right relation = left relation * ratio
    left_size=[2048_000],
)

@reg_exp(servers=server_list, params=parameter_grid)
def distributed_radix_join(servers, ratio, left_size):

    GDB = "gdb -ex 'set print thread-events off' -ex run --args"
    BIN ='./build/bin/distributed_replicate_join_flow'
    L_REL_SIZE = left_size
    R_REL_SIZE = left_size * ratio
    RANDOM_ORDER = 't'

    set_dfi_conf(servers, len(servers) * len(NUMA_NODES))
    compile(servers, L_REL_SIZE)

    # controller = StdinController()
    procs = []
    matchers = IterClassGen(SubstrMatcher, 'Press any key to run...')
    node_id = counter(0)


    csvs = IterClassGen(CSVGenerator,
        r'node=(?P<node>\d+)',
        r'nodes=(?P<nodes>\d+)',
        r'full_left_rel=(?P<full_left_rel>\d+(?:\.\d+)?)',
        r'full_right_rel=(?P<full_right_rel>\d+(?:\.\d+)?)',
        r'local_parts=(?P<local_parts>\S+)',
        r'part_threads=(?P<part_threads>\d+(?:\.\d+)?)',
        r'dfi_segment_width=(?P<dfi_segment_width>\d+(?:\.\d+)?)',
        r'bandwidth=(?P<bandwidth_left>\d+(?:\.\d+)?)',
        r'time_network=(?P<time_network>\d+(?:\.\d+)?)',
        r'time_local_part=(?P<time_local_part>\d+(?:\.\d+)?)',
        r'time_build_probe=(?P<time_build_probe>\d+(?:\.\d+)?)',
        r'total_time=(?P<total_time>\d+(?:\.\d+)?)',
        r'random_order=(?P<random_order>\S+?)',
        r'sum_join_count=(?P<sum_join_count>\d+)',
        CSVGenerator.Sum(r'sender_stalls=(?P<sender_stalls>\d+)'),
        CSVGenerator.Mean(r'sender_stall_sent_ratio=(?P<sender_stall_sent_ratio_mean>\d+(?:\.\d+)?)'),
        r'non_temp_writes=(?P<non_temp_writes>\S+)',
    )

    for s in servers:
        for numa_region in NUMA_NODES:
            threads = 14
            # cmd = f'{GDB} {BIN} {next(node_id)} {len(servers) * len(NUMA_NODES)} {threads} {L_REL_SIZE} {R_REL_SIZE} {RANDOM_ORDER} {numa_region}'
            cmd = f'{BIN} {next(node_id)} {len(servers) * len(NUMA_NODES)} {threads} {L_REL_SIZE} {R_REL_SIZE} {RANDOM_ORDER} {numa_region}'
            stdout = (next(matchers), next(csvs))
            # procs.append(s.run_cmd(cmd, stdout=stdout, stdin=controller)) ## For debug
            procs.append(s.run_cmd(cmd, stdout=stdout))

    all(m.wait() for m in matchers)
    log('Pressing enter automatically')
    [p.stdin('\n') for p in procs]        

    # controller.wait()

    assert(all(p.wait() == 0 for p in procs))

    for csv in csvs:
        csv.write('results.csv')