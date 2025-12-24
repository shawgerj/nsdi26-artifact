#!/bin/bash

# Update myexperiment to match the experiment name and size of DB.
# We used 50GB in the paper since this experiment is time-consuming.

source ./common.sh

produce_graph () {
    graphtype=$1
    alldbs="${dbs[@]}"
    
    /usr/bin/gnuplot <<EOF
set terminal pdf enhanced size 3.7in,1.9in
set output "$graphsdir/$graphtype-write.pdf"
set xlabel "Throughput (MB/Sec)"
set ylabel "Median Latency (ms)"
set xrange [0:*]
set yrange [0:10]

set style line 1 lc rgb "black" dt 1 lw 2
set style line 2 lc rgb "black" dt 2 lw 2
set style line 3 lc rgb "black" dt 4 lw 2

titles = "TiKV XLL-SO XLL"
styles = "1 2 3"
dbs = "$alldbs"

set multiplot layout 1,2
set title "1KB Records"
set yrange [0:4]
vsize = 1024
set xtics 20
plot for [i=1:words(dbs)] sprintf("%s/%s-%s-1KB.dat", "$datadir", word(dbs, i), "$graphtype") \
using (((\$1 + \$2)*vsize)/(1024*1024)):(((\$3 + \$4)/2)/1000) w lines ls word(styles, i) title word(titles, i)

set title "16KB Records"
set key off
unset ylabel
vsize = 16384
set xtics 50
set yrange [0:5]
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

    for db in ${dbs[@]}; do
	echo "$db"
	outfile="$datadir/$db-$myexperiment-$vsizestr.dat"
	for threads in $(seq 4 2 64); do
	    ops=()
	    latency=()
	    for client in $(seq 0 1); do
		# we have multiple runs of 16KB write workload
		if [ "$vsizestr" = "16KB" ]; then
		    ops_temp=()
		    latency_temp=()
		    for run in $(seq 3); do
			filepath=$(experiment_directory_multirun "$db" "$myexperiment" "$run" "$vsizestr")/writescalability/load_threads_${threads}_client_$client.ycsb
			if [[ -f "$filepath" ]]; then
			    read clientops clientlatency < <(read_ops_latency "$filepath")
			    ops_temp+=("$clientops")
			    latency_temp+=("$clientlatency")
			fi
		    done
		    if [ ${#ops_temp[@]} -gt 0 ]; then
			found=1
			ops[$client]=$(printf '%s\n' "${ops_temp[@]}" | awk '{sum += $1} END {print sum/NR}')
			latency[$client]=$(printf '%s\n' "${latency_temp[@]}" | awk '{sum += $1} END {print sum/NR}')
		    else
			found=0
		    fi
		else
		    filepath=$(experiment_directory "$db" "$myexperiment" "$vsizestr")/writescalability/load_threads_${threads}_client_$client.ycsb
		    if [[ -f "$filepath" ]]; then
			found=1
			read clientops clientlatency < <(read_ops_latency "$filepath")
			ops[$client]=$clientops
			latency[$client]=$clientlatency
		    else
			found=0
			continue
		    fi
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
myexperiment="write-scalability-50GB"

for vsize in ${valuesizes[@]}; do
    vsizestr=$(value_string $vsize)
    for db in ${dbs[@]}; do
	rm $datadir/$db-$myexperiment-$vsizestr.dat
    done
    
    generate_data $vsize
done

produce_graph $myexperiment
# the read (YCSB C) results aren't very interesting so I'm not graphing them
