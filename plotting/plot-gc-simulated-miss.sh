#!/bin/bash

# We don't have great tooling for this experiment. For Fig. 13 in the paper,
# edit gc_hitrate manually in components/engine_rocks/src/raft_engine.rs  and
# run the YCSB load experiment, naming them xll-gc-<rate>-hitrate.
#

source ./common.sh
dbs=("tikv" "xll")
datafile=$datafiles/gc-simulated-hitrate.dat

produce_graph () {
    graphtype=$1

    /usr/bin/gnuplot <<EOF
set terminal cairolatex pdf size 3.7in,1.9in
set output "$graphsdir/$graphtype.pdf"

set xlabel "WOTR Garbage Collection Hit Rate"
set ylabel "Throughput (MB/Sec)"
set xrange [0:1]
set yrange [0:*]

set style line 1 lc rgb "black" dt 1 lw 2
set style line 2 lc rgb "black" dt 2 lw 2
set style line 3 lc rgb "black" dt 4 lw 2

titles = "TiKV TiKV-XLL"
styles = "1 3"
dbs = "$alldbs"

vsize = 16384
tikvrate=system("awk '/tikv/ {print \$2}' $datafile")
xllrate=system("awk '/xll/ {print \$2}' $datafile")

set arrow from 0,((tikvrate*vsize)/(1024*1024)) to 1,((tikvrate*vsize)/(1024*1024)) ls 1 nohead
set arrow from 0,((xllrate*vsize)/(1024*1024)) to 1,((xllrate*vsize)/(1024*1024)) ls 3 nohead

set label "XLL (No GC)" at 0.05,((wotrrate*vsize)/(1024*1024))+16
set label "TiKV" at 0.05,((tikvrate*vsize)/(1024*1024))+16
set label "XLL-GC" at 0.75,((wotrrate*vsize)/(1024*1024))-75

plot "<awk '/GC/ {print \$0}' $datafile" using (1-\$3):(\$2*vsize/(1024*1024)) w lines ls 2 notitle

EOF
}
generate_data () {
    rm $datafile

    # GC always on
    awk -v hitrate=$i 'BEGIN { FS = ", " } /TOTAL/ {split($3,ops," "); split($7,latency," "); print "GC", ops[2], "0.0"}' $resultsdir/xll-gc-2-seconds-50GB-16KB/ycsb/load_threads_32_client_0.ycsb | tail -n1 >> $datafile
    
    # simulated miss percentages
    for i in $(seq 0.2 0.2 1); do
	awk -v hitrate=$i 'BEGIN { FS = ", " } /TOTAL/ {split($3,ops," "); split($7,latency," "); print "GC", ops[2], hitrate}' $resultsdir/xll-gc-${i}-hitrate-50GB-16KB/ycsb/load_threads_32_client_0.ycsb | tail -n1 >> $datafile
    done

    for db in ${dbs[@]}; do
	# YCSB LOAD (write-only)
	awk -v db=$db 'BEGIN { FS = ", " } /TOTAL/ {split($3,ops," "); split($7,latency," "); print db, ops[2]}' $resultsdir/$db-writescalability-50GB-16KB/ycsb/load_threads_32_client_0.ycsb | tail -n1 >> $datafile
    done
}

generate_data
produce_graph "gc-simulated-hitrate"
