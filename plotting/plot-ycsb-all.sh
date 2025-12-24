#!/bin/bash

# edit `myexperiment` to match experiment name and db size
# i.e., "ycsb-100GB". tikv-ycsb.py will already add the db size in
# GB to the directory name, so it needs to match here.
source ./common.sh
workloads=("run_a" "run_b" "run_c" "run_d" "run_e" "run_f")

produce_graph () {
    vsize=$1
    vsizestr=$(value_string $vsize)
    keyctrl=""
    data=$datadir/$myexperiment-$vsizestr.dat
    echo $data
    if [ $vsize -eq 16384 ]; then
	keyctrl="#"
    fi
    
    /usr/bin/gnuplot <<EOF
set terminal pdf enhanced size 3.2in,1.7in
set output "$graphsdir/ycsb-$vsize-single-node-100G.pdf"

set style fill solid
set boxwidth 0.2
set key top center
$keyctrl set key off

set ylabel "Normalized Throughput"
set yrange [0:7]

set style line 1 lc rgb "black"

plot "$data" using 1:(\$3/\$3):xtic(2) title "TiKV" with boxes ls 1 fs pattern 1, \
     "$data" using (\$1+0.2):(\$4/\$3) title "XLL-SO" with boxes ls 1 fs pattern 2, \
     "$data" using (\$1+0.4):(\$5/\$3) title "XLL" with boxes ls 1 fs pattern 3, \
     "<awk 'BEGIN {OFMT=\"\%.1f\"} {print \$1,\$5/1000,\$3,\$4,\$5}' $data" using (\$1+0.2):(\$5/\$3):2 with labels offset char 0,0.9 title " "
EOF
}

read_ops () {
    workload=$1
    vsize=$2
    db=$3
    c=$4

    if [ "$vsizestr" = "16KB" ]; then
	threads=32
    else
	threads=64
    fi

    filepath=$(experiment_directory "$db" "$myexperiment" "$vsizestr")/ycsb/${workload}_threads_${threads}_client_$c.ycsb
    
    if [ -f $filepath ]; then
	echo $(awk 'BEGIN { FS = ", " } /TOTAL/ {split($3,ops," "); print ops[2]}' $filepath | tail -n1)
    else
	echo 0
    fi
}
    
generate_data () {
    vsize=$1
    vsizestr=$(value_string $vsize)
    outfile=$datadir/$myexperiment-$vsizestr.dat
    i=0
    ops=()

    echo -en "${i}\tLOAD" >> $outfile
    for db in ${dbs[@]}; do
	for client in $(seq 0 1); do
	    # YCSB LOAD (write-only)
	    ops[$client]=$(read_ops "load" $vsize $db $client)
	done
	echo -en "\t$(bc <<< "${ops[0]} + ${ops[1]}")" >> $outfile
    done
    echo -en "\n" >> $outfile
    
    ((i+=1))

    for w in ${workloads[@]}; do
	wletter=${w: -1}
	echo -en "${i}\t${wletter^^}" >> $outfile
	for db in ${dbs[@]}; do
	    for client in $(seq 0 1); do
		ops[$client]=$(read_ops "$w" $vsize $db $client)
	    done
	    echo -en "\t$(bc <<< "${ops[0]} + ${ops[1]}")" >> $outfile
	done
	echo -en "\n" >> $outfile
	((i+=1))
    done
}

mkdir -p $datadir
mkdir -p $graphsdir
myexperiment="ycsb-20GB"


for vsize in ${valuesizes[@]}; do
    vsizestr=$(value_string $vsize)
    rm $datadir/$myexperiment-$vsizestr.dat
    
    generate_data $vsize
    produce_graph $vsize    
done
