#!/bin/bash

resultsdir="../results"
datadir="../dot-dat"
graphsdir="../graphs"
valuesizes=(1024 16384)
dbs=("tikv" "xllso" "xll")

# args
# $1: system name
# $2: experiment name
# $3: value size
experiment_directory () {
    echo "$resultsdir/$1-$2-$3"
}

# args
# $1: system name
# $2: experiment name
# $3: run number
# $4: db size
# $5: value size
experiment_directory_multirun () {
    echo "$resultsdir/$1-$2-$3-$4-$5"
}

value_string () {
    echo "$(($1/1024))KB"
}

