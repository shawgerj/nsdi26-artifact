#!/bin/bash

source ./common.sh
mydbs=("wotr" "wisckey")
# experimentroot="../results"
# datafiles="./dot-dat"
# graphfiles="./graphs"
# valuesizes=(16384)
# dbs=("wotr")

produce_graph () {
    graphtype=$1
    alldbs="${mydbs[@]}"
    
    /usr/bin/gnuplot <<EOF
set terminal pdf enhanced size 3.7in,1.9in
set output "$graphtype.pdf"
set xlabel "Write Proportion \%"
set xrange [0:*]
set xtics autofreq 0.2

set style line 1 lc rgb "black" dt 1 lw 2
set style line 2 lc rgb "black" dt 2 lw 2
set style line 3 lc rgb "black" dt 4 lw 2

titles = "XLL XLL-SO"
styles = "3 2"
dbs = "$alldbs"

set multiplot layout 1,2
set title "Write Throughput"
set key bottom right
set ylabel "Ops/sec"
set yrange [0:*]
plot for [i=1:words(dbs)] sprintf("%s/update-16384-%s.dat", "$datadir", word(dbs, i)) \
using 1:(\$2 + \$3) w lines ls word(styles, i) title word(titles, i)

set title "Read Throughput"
set key off
set ylabel "Ops/sec"
set yrange [0:*]
plot for [i=1:words(dbs)] sprintf("%s/update-16384-%s.dat", "$datadir", word(dbs, i)) \
using 1:(\$4 + \$5) w lines ls word(styles, i) title word(titles, i)

unset multiplot
EOF
}

read_ops () {
    operation=$1
    filepath=$(experiment_directory "$db" "$myexperiment" "50GB" "$vsizestr")/updateproportion/update_${wprop}_read_${rprop}_client_$client.ycsb

    echo $(awk -v op="$operation" 'BEGIN { FS = ", " } $0 ~ op {split($3,ops," "); print ops[2]}' $filepath | tail -n1)
}
generate_data () {
    vsize=$1
    vsizestr=$(value_string $vsize)

    for db in ${mydbs[@]}; do
	outfile=$datadir/update-$vsize-$db.dat
	updateops=()
	readops=()
	for wprop in $(seq 0.1 0.1 0.9); do
	    for client in $(seq 0 1); do
		rprop=$(awk -v wprop=$wprop 'BEGIN { printf "%.1f", 1.0 - wprop }')

		read clientops < <(read_ops "UPDATE")
		updateops[$client]=$clientops
		read clientops < <(read_ops "READ")
		readops[$client]=$clientops
		# gets "num ops, median latency" for update operations
		# awk -v wprop=$wprop 'BEGIN { FS = ", " } /UPDATE/ {split($3,ops," "); split($7,latency," "); print wprop, ops[2], latency[2]}' $experimentroot/$db-write-variable_$vsize/update_${wprop}_read_${rprop}_$vsize.ycsb | tail -n1 >> $datafiles/update-$vsize-$db.dat
	    done
	    echo $wprop ${updateops[0]} ${updateops[1]} ${readops[0]} ${readops[1]} >> $outfile
	done
    done
}

mkdir -p $datadir
mkdir -p $graphsdir
myexperiment="update-proportion"

for db in ${mydbs[@]}; do
    rm $datadir/update-16384-$db.dat
done

generate_data 16384

produce_graph "variable-write-read"

