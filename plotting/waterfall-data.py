# Expects experiment names (passed with --name argument to tikv-ycsb.py)
# "tikv-write-scalability" and "wotr-write-scalability" (wotr=XLL, this script
# uses an old name). 
#
# --experimenttype writescalability --threadsmin 16 --threads 128

import os
import re
import subprocess
from pathlib import Path

experimentroot = '../results'
experiments = ['tikv-write-scalability-50GB-1KB', 'wotr-write-scalability-50GB-1KB']

# list of metrics taken from components/raftstore/src/store/metrics.rs
wf_metrics = ['tikv_raftstore_store_wf_batch_wait_duration_seconds',
              'tikv_raftstore_store_wf_send_to_queue_duration_seconds',
              'tikv_raftstore_store_wf_before_write_duration_seconds',
              'tikv_raftstore_store_wf_write_kvdb_end_duration_seconds',
              'tikv_raftstore_store_wf_write_end_duration_seconds',
              'tikv_raftstore_store_wf_persist_duration_seconds',
              'tikv_raftstore_store_wf_commit_log_duration_seconds']

phase_metrics = ['tikv_raftstore_store_duration_secs',
                 'tikv_raftstore_apply_duration_secs',
                 'tikv_raftstore_append_log_duration_seconds',
                 'tikv_raftstore_commit_log_duration_seconds',
                 'tikv_raftstore_apply_wait_time_duration_secs']

def metrics_file(exp):
    """Helper function to get metrics file path for an experiment"""
    return f'{experimentroot}/{exp}/writescalability/0/tikv.metrics'


def get_count(data, metric):
    count_pattern = f'{metric}_count'
    count_match = re.search(rf'^.*{re.escape(count_pattern)}.*\s+(\d+)$', data, re.MULTILINE)
    if not count_match:
        print(f"Error: Could not find {count_pattern}")
        return 0
    return int(count_match.group(1))

def get_buckets(data, metric):
    bucket_pattern = f"{metric}_bucket"
    buckets = []
    for line in data.split('\n'):
        if bucket_pattern in line and 'Inf' not in line:
            match = re.search(r'le="([^"]*)".*\s+(\d+)$', line)
            if match:
                le_value = float(match.group(1))
                count = int(match.group(2))
                buckets.append((le_value, count))

    return buckets
    
def get_median_time(metric, experiment):
    """
    Get histogram bucket holding the median value for a particular metric.

    Args:
        metric (str)
        experiment (str)
    """
    with open(metrics_file(experiment), 'r') as f:
        content = f.read()

    total_count = get_count(content, metric)
    histogram_data = get_buckets(content, metric)
    target = total_count // 2

    for time, count in histogram_data:
        if count >= target:
            return time * 1000

    # if our histogram didn't capture enough data
    print(f'WARN {experiment} {metric} median value not found')
    return 0

def run_gnuplot(script):
    try:
        process = subprocess.run(['gnuplot'], input=script, 
                               text=True, check=True, capture_output=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running gnuplot: {e}")
        print(f"gnuplot stderr: {e.stderr}")
    except FileNotFoundError:
        print("Error: gnuplot not found. Please install gnuplot.")
    
def produce_stacked_histogram(datafile):
    output = 'raftcommit.pdf'
    script = f'''set terminal pdf size 5in,3in
    set output "{output}"
    set title "Raft Commit Metrics"
    set tics nomirror
    set xlabel "System"
    set ylabel "Commit Time"
    set yrange [0:*]
    set xrange [-0.5:1.5]
    set key autotitle columnheader
    set key outside right
    set style data histograms
    set style histogram rowstacked
    set boxwidth 0.5
    set style fill pattern border -1

    plot '{datafile}' using 2:xtic(1) title columnheader(2) ls 2, \
    '' using 3 title columnheader(3) ls 3, \
    '' using 4 title columnheader(4) ls 4, \
    '' using 5 title columnheader(5) ls 5, \
    '' using 6 title columnheader(6) ls 6, \
    '' using 7 title columnheader(7) ls 7, \
    '' using 8 title columnheader(8) ls 8

    '''
    run_gnuplot(script)
    
def generate_display_name(metric):
    components = [c.capitalize() for c in metric.split('_')]
    return ''.join(components[4:-2])

def generate_cdf_display_name(metric):
    components = [c.capitalize() for c in metric.split('_')]
    return ''.join(components[2:-2])

def write_cdf_data(buckets, count, metric, name):
    """
    Write cumulative probabilities to a file for each histogram bucket.
    """
    outfile = f'{name}-{metric}.dat'
    with open(outfile, 'w') as f:
        for time, c in buckets:
            f.write(f'{time * 1000:.6f}\t{c}\t{c / count:.6f}\n')

def generate_cdf_plot(metric):
    print(generate_cdf_display_name(metric))
    script = f'''set terminal pdf size 5in,3in
    set output "{generate_cdf_display_name(metric)}_cdf.pdf"
    set title "{generate_cdf_display_name(metric)} CDF"
    unset xlabel
    set ylabel "Cumulative Probability"
    set yrange [0:1]
    set xrange [0:*]
    set key bottom right

    plot "tikv-{metric}.dat" using 1:3 with steps title "tikv", \
         "wotr-{metric}.dat" using 1:3 with steps title "wotr
    '''
    run_gnuplot(script)

def generate_multiplot():
    """
    Generate a multiplot with two most interesting metrics, store_duration_secs and apply_duration_secs.
    """
    script = f'''set terminal pdf size 4.5in,3in
    set output "request_coarse_phase_cdf.pdf"
    set ylabel "Cumulative Probability"
    set yrange [0:1]
    set xrange [0:*]
    set tics nomirror

    set style line 1 lc rgb "black" dt 1 lw 2
    set style line 2 lc rgb "black" dt 4 lw 2

    left_width = 0.46
    left_height = 0.49
    left_x = 0.02
    gap = 0.02

    right_width = 0.45
    right_height = 1.0
    right_x = 0.52

    set multiplot
    set title "Propose + Commit"
    set key off
    set size left_width, left_height
    set origin left_x, 0.5 + gap/2
    plot "tikv-tikv_raftstore_store_duration_secs.dat" using 1:3 with steps ls 1 title "TiKV", \
         "wotr-tikv_raftstore_store_duration_secs.dat" using 1:3 with steps ls 2 title "XLL"

    set title "Apply"
    set key bottom right
    set xlabel "Time (msec)"
    set size left_width, left_height
    set origin left_x, 0.02
    plot "tikv-tikv_raftstore_apply_duration_secs.dat" using 1:3 with steps ls 1 title "TiKV", \
         "wotr-tikv_raftstore_apply_duration_secs.dat" using 1:3 with steps ls 2 title "XLL"

    set size right_width, right_height
    set origin right_x, 0.02
    
    set title "Consensus Phases - Median Op"
    unset xlabel
    set ylabel "Commit Time (msec)"
    set yrange [0:1.5]
    set xrange [-0.5:1.5]
    set key autotitle columnheader
    set key below
    set style data histograms
    set style histogram rowstacked
    set boxwidth 0.5
    set style fill pattern border -1

    plot 'raftcommit_metrics.dat' using ($2+$3):xtic(1) title 'Propose' ls 9, \
    '' using ($4+$5+$6) title 'Commit-disk' ls 4, \
    '' using 8 title 'Commit-followers' ls 8, \
    '' using 9 title 'Apply' ls 10
    
    '''
    run_gnuplot(script)
    
if __name__ == "__main__":
    # waterfall metrics (detailed breakdown of commit phase)
    outfile = f"raftcommit_metrics.dat"
    with open(outfile, 'w') as f:
        displaymetrics = [generate_display_name(metric) for metric in wf_metrics]
        metricstr = '\t'.join(displaymetrics)
        f.write(f'Experiment\t{metricstr}\tApplyTime\tApplyWait\n')

        for experiment in experiments:
            name = experiment.split('-')[0]
            if name == "wotr":
                name = "XLL"
            f.write(f'{name.upper()}')
            prev = 0.0
            for metric in wf_metrics:
                time = get_median_time(metric, experiment)
                f.write(f'\t{time - prev:.6f}')
                prev = time

            # phase metrics CDFs
            for metric in phase_metrics:
#                for experiment in experiments:
                with open(metrics_file(experiment), 'r') as fmetrics:
                    content = fmetrics.read()
                    
                    count = get_count(content, metric)
                    buckets = get_buckets(content, metric)
                    median = get_median_time(metric, experiment)
                    if 'apply' in metric:
                        f.write(f'\t{median}')
                        
                    write_cdf_data(buckets, count, metric, experiment.split('-')[0])
            f.write('\n')

    generate_multiplot()
