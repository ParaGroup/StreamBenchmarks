#!/bin/bash
# Launcher of the Taxi experiments on WindFlow

# Working directory
MYDIR=${HOME}/StreamBenchmarks/WindFlow/TaxiQueries

echo "Creating the output directories for storing the results --> OK!"
mkdir -p ${MYDIR}/results
mkdir -p ${MYDIR}/results_db
mkdir -p ${MYDIR}/results_db_cache

# Tests in-memory
echo "Running tests on WindFlow - version with in-memory processing ..."
cd ${MYDIR}
for q in 1 2 3; do
    for p in 1 2 4 8; do 
        for t in 1 2 3 4; do
            echo "./bin/taxi_queries -q ${q} -p ${p} -b 0"
            ./bin/taxi_queries -q ${q} -p ${p} -b 0 > output-q${q}-p${p}_t${t}.log
            th=$(grep "throughput" output-q${q}-p${p}_t${t}.log | cut -f3 -d " " | awk '{ sum += $1 } END { print sum }')
            echo "Measured throughput: ${th}"
            echo "$th" >> ${MYDIR}/results/bw-q${q}_p${p}.log
        done
    done
done

echo "...end"

# Tests RocksDB
echo "Running tests on WindFlow - version with persistent processing ..."
cd ${MYDIR}
for q in 1 2 3; do
    for p in 1 2 4 8; do 
        for t in 1 2 3 4; do
            echo "./bin/taxi_queries -q ${q} -p ${p} -b 0 -r"
            ./bin/taxi_queries -q ${q} -p ${p} -b 0 -r > output-q${q}-p${p}_t${t}.log
            th=$(grep "throughput" output-q${q}-p${p}_t${t}.log | cut -f3 -d " " | awk '{ sum += $1 } END { print sum }')
            echo "Measured throughput: ${th}"
            echo "$th" >> ${MYDIR}/results_db/bw-q${q}_p${p}.log
        done
    done
done

echo "...end"

# Tests RocksDB with caching
echo "Running tests on WindFlow - version with persistent processing with caching ..."
cd ${MYDIR}
for q in 1 2 3; do
    for p in 1 2 4 8; do 
        for t in 1 2 3 4; do
            echo "./bin/taxi_queries -q ${q} -p ${p} -b 0 -r -c 100"
            ./bin/taxi_queries -q ${q} -p ${p} -b 0 -r -c 100 > output-q${q}-p${p}_t${t}.log
            th=$(grep "throughput" output-q${q}-p${p}_t${t}.log | cut -f3 -d " " | awk '{ sum += $1 } END { print sum }')
            echo "Measured throughput: ${th}"
            echo "$th" >> ${MYDIR}/results_db_cache/bw-q${q}_p${p}.log
        done
    done
done

echo "...end"
