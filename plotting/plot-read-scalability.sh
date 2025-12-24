#!/bin/bash

# This script uses an extra experiment "xll-slow-read" on branch 
# "tikv-xll-slow-read" to compare against no read optimization.
# Edit myexperiment to match experiment result directory.

source ./common.sh
mydbs=("tikv" "xllso" "xll" "xll-slow-read")

produce_graph () {
    graphtype=$1
    alldbs="${mydbs[@]}"
    
    /usr/bin/gnuplot <<EOF
set terminal pdf enhanced size 3.7in,1.9in
set output "$graphsdir/$graphtype-read.pdf"
set xlabel "Throughput (MB/Sec)"
set ylabel "Median Latency (ms)"
set xrange [0:*]
set yrange [0:10]

set style line 1 lc rgb "black" dt 1 lw 2
set style line 2 lc rgb "black" dt 2 lw 2
set style line 3 lc rgb "black" dt 4 lw 2
set style line 4 lc rgb "black" dt 3 lw 2

titles = "TiKV XLL-SO XLL XLL-SR"
styles = "1 2 3 4"
dbs = "$alldbs"

set multiplot layout 1,2
set title "1KB Records"
set yrange [0:2]
vsize = 1024
plot for [i=1:words(dbs)] sprintf("%s/%s-%s-1KB.dat", "$datadir", word(dbs, i), "$graphtype") \
using (((\$1 + \$2)*vsize)/(1024*1024)):(((\$3 + \$4)/2)/1000) w lines ls word(styles, i) title word(titles, i)

set title "16KB Records"
set key off
set xtics 300
unset ylabel
vsize = 16384
set yrange [0:2]
plot for [i=1:words(dbs)] sprintf("%s/%s-%s-16KB.dat", "$datadir", word(dbs, i), "$graphtype") \
using (((\$1 + \$2)*vsize)/(1024*1024)):(((\$3+\$4)/2)/1000) w lines ls word(styles, i) title word(titles, i)

unset multiplot
EOF
}

read_ops_latency () {
    awk 'BEGIN { FS = ", " } /TOTAL/ {split($3,ops," "); split($7,latency," "); print ops[2], latency[2]}' $1 | tail -n1
}

generate_data () {
    vsize=$1
    vsizestr=$(value_string $vsize)

    for db in ${mydbs[@]}; do
	echo "$db"
	outfile="$datadir/$db-$myexperiment-$vsizestr.dat"
	for threads in $(seq 4 4 64); do
	    ops=()
	    latency=()
	    for client in $(seq 0 1); do
		filepath=$(experiment_directory "$db" "$myexperiment" "$vsizestr")/ycsb/run_c_threads_${threads}_client_$client.ycsb
		if [[ -f "$filepath" ]]; then
		    found=1
		    read clientops clientlatency < <(read_ops_latency "$filepath")
		    ops[$client]=$clientops
		    latency[$client]=$clientlatency
		else
		    found=0
		    continue
		fi
	    done

	    # columns: CLIENT0_OPS CLIENT1_OPS CLIENT0_LAT50 CLIENT1_LAT50
	    if [[ $found -eq 1 ]]; then
		echo ${ops[0]} ${ops[1]} ${latency[0]} ${latency[1]} >> $outfile
	    fi
	done	
    done
}

mkdir -p $datadir
myexperiment="read-scalability-100GB"

for vsize in ${valuesizes[@]}; do
    vsizestr=$(value_string $vsize)
    for db in ${mydbs[@]}; do
	rm $datadir/$db-$myexperiment-$vsizestr.dat
    done
    
    generate_data $vsize
done

produce_graph $myexperiment
