#!/bin/bash

APPNAME="rsm_nopaxos"
pkill -f "bin/$APPNAME" # kill old instance

CMD_COUNT=1000000
REPITITIONS=1

CPS_FROM=50000
CPS_TO=50000
CPS_STEP=50000

REPLICA_COUNT=5
CLIENT_COUNT=6
#REPLICA_IP_LIST="172.18.94.10 172.18.94.20 172.18.94.30 172.18.94.40 172.18.94.50 172.18.94.11 172.18.94.21 172.18.94.31 172.18.94.41 172.18.94.51 172.18.94.60 172.18.94.70 172.18.94.80 172.18.94.61 172.18.94.71 172.18.94.81"
REPLICA_IP_LIST="172.18.94.10 172.18.94.20 172.18.94.30 172.18.94.40 172.18.94.50 172.18.94.60 172.18.94.70 172.18.94.80 172.18.94.61 172.18.94.71 172.18.94.81"
BENCHNAME="${APPNAME}_r${REPLICA_COUNT}_c${CLIENT_COUNT}_max"

DFI_PATH="$HOME/dfi_library_private"
DFI_BUILD="$DFI_PATH/build"
APP_EXEC="$DFI_BUILD/bin/$APPNAME"
BENCHMARK_DIR="$DFI_PATH/_consensus_bench_data"
LOG_DIR="$BENCHMARK_DIR/log"

NODE_COUNT=$((REPLICA_COUNT + CLIENT_COUNT))

kill_dfi() {
    echo "################# Killing running instances of DFI"
    for i in `seq $NODE_COUNT -1 1`;
    do
        NODE="$(echo $REPLICA_IP_LIST | cut -d' ' -f$i)"
        ssh $NODE "pkill -f bin/$APPNAME"&
    done
    wait

    echo "################# DFI killed"
}

abort_bench() {
    >&2 echo "ERROR: Aborting benchmark (manually or something went wrong)"
    kill_dfi
    echo "################# Everything cleared"
    exit 1
}

is_local() {
    ifconfig | grep $1 | wc -l
}

trap abort_bench SIGINT SIGTERM


echo "################# Creating benchmark directory $BENCHMARK_DIR/$BENCHNAME"
mkdir -p $BENCHMARK_DIR/$BENCHNAME
mkdir -p $LOG_DIR

echo "################# Synching directories"
for i in `seq 1 $NODE_COUNT`;
do
    NODE="$(echo $REPLICA_IP_LIST | cut -d' ' -f$i)"
    if [ `is_local $NODE` -eq 0 ];
    then
        echo "Synching to node $NODE"
        rsync -av --exclude build/ $DFI_PATH $NODE:$HOME/ --delete-after || abort_bench
    fi
done
wait

echo "################# compiling DFI"
for i in `seq 1 $NODE_COUNT`;
do
    echo "Compiling on node $NODE"
    NODE="$(echo $REPLICA_IP_LIST | cut -d' ' -f$i)"
    ssh $NODE "make -C $DFI_PATH/build -j8" || abort_bench
done
wait

for CMD_PER_SECOND in `seq $CPS_FROM $CPS_STEP $CPS_TO`;
do
for r in `seq 1 $REPITITIONS`;
do
    echo "################# Starting repitition $r ($CMD_PER_SECOND commands per second)"

    kill_dfi

    echo "################# Executing runs"
    for i in `seq 1 1 $NODE_COUNT`;
    do
        BENCHFILE="$BENCHMARK_DIR/$BENCHNAME/node-tp$CMD_PER_SECOND-r$r-n$i.data"
        NODE="$(echo $REPLICA_IP_LIST | cut -d' ' -f$i)"
        echo "Starting on node $NODE"
                
        ssh $NODE "$APP_EXEC $BENCHFILE $CMD_COUNT $CMD_PER_SECOND $i $REPLICA_COUNT $CLIENT_COUNT $REPLICA_IP_LIST" | tee -a $LOG_DIR/node$i.log&
    done
    wait

    kill_dfi

    echo "################# Collecting benchmark results"
    for i in `seq 1 1 $NODE_COUNT`;
    do
        NODE="$(echo $REPLICA_IP_LIST | cut -d' ' -f$i)"

        BENCHFILE="$BENCHMARK_DIR/$BENCHNAME/node-tp$CMD_PER_SECOND-r$r-n$i.data"
        TO="$BENCHMARK_DIR/$BENCHNAME/"
        scp "$NODE:$BENCHFILE" $TO
    done
done
done