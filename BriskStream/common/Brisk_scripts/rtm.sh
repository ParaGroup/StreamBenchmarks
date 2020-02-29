#!/bin/bash
searchdir="--search-dir all:rp=$HOME/Documents/briskstream --search-dir all:rp=$JAVA_HOME/lib  --search-dir all:rp=/opt/intel/vtune_amplifier_xe_2017/bin64"
function profile {

    cnt=0
    while [ ! -s  $2/sink_threadId.txt ]
        do
            echo "wait for sink id $cnt"
            let cnt=cnt+1
            sleep 1
    done
    r=$(<$2/sink_threadId.txt)

	echo "$r"
	jstack $r >> $2/threaddump.txt
	case $1 in
	1)	#General Exploration with CPU transaction and Memory Bandwidth
		#amplxe-cl -collect general-exploration -knob collect-memory-bandwidth=true -target-duration-type=medium -data-limit=1024 -duration=100 $searchdir  --start-paused --resume-after 10 --target-pid  $r -result-dir $MY_PATH2/resource >> $MY_PATH2/profile1.txt;;
		amplxe-cl -collect general-exploration -knob collect-memory-bandwidth=true -target-duration-type=medium -duration=30 --target-pid $r -result-dir $2/general >> $2/profile_type1.txt ;;
	2)	#general
		amplxe-cl -collect-with runsa -knob event-config=CPU_CLK_UNHALTED.THREAD_P:sa=2000003,DTLB_LOAD_MISSES.STLB_HIT:sa=100003,DTLB_LOAD_MISSES.WALK_DURATION:sa=2000003,ICACHE.MISSES:sa=200003,IDQ.EMPTY:sa=2000003,IDQ_UOPS_NOT_DELIVERED.CORE:sa=2000003,ILD_STALL.IQ_FULL:sa=2000003,ILD_STALL.LCP:sa=2000003,INST_RETIRED.ANY_P:sa=2000003,INT_MISC.RAT_STALL_CYCLES:sa=2000003,INT_MISC.RECOVERY_CYCLES:sa=2000003,ITLB_MISSES.STLB_HIT:sa=100003,ITLB_MISSES.WALK_DURATION:sa=2000003,LD_BLOCKS.STORE_FORWARD:sa=100003,LD_BLOCKS_PARTIAL.ADDRESS_ALIAS:sa=100003,MEM_LOAD_UOPS_LLC_HIT_RETIRED.XSNP_HIT:sa=20011,MEM_LOAD_UOPS_LLC_HIT_RETIRED.XSNP_HITM:sa=20011,MEM_LOAD_UOPS_LLC_MISS_RETIRED.REMOTE_DRAM:sa=100007,MEM_LOAD_UOPS_RETIRED.L1_HIT_PS:sa=2000003,MEM_LOAD_UOPS_RETIRED.L2_HIT_PS:sa=100003,MEM_LOAD_UOPS_RETIRED.LLC_HIT:sa=50021,MEM_LOAD_UOPS_RETIRED.LLC_MISS:sa=100007,MEM_UOPS_RETIRED.SPLIT_LOADS_PS:sa=100003,MEM_UOPS_RETIRED.SPLIT_STORES_PS:sa=100003,OFFCORE_REQUESTS.ALL_DATA_RD:sa=100003,OFFCORE_RESPONSE.ALL_DEMAND_MLC_PREF_READS.LLC_MISS.ANY_RESPONSE_1:sa=100003,OFFCORE_RESPONSE.ALL_DEMAND_MLC_PREF_READS.LLC_MISS.LOCAL_DRAM_0:sa=100003,OFFCORE_RESPONSE.ALL_DEMAND_MLC_PREF_READS.LLC_MISS.REMOTE_HITM_HIT_FORWARD_1:sa=100003,OFFCORE_RESPONSE.PF_L2_DATA_RD.LLC_MISS.ANY_DRAM_0:sa=100003,PARTIAL_RAT_STALLS.FLAGS_MERGE_UOP_CYCLES:sa=2000003,PARTIAL_RAT_STALLS.SLOW_LEA_WINDOW:sa=2000003,RESOURCE_STALLS.ANY:sa=2000003,RESOURCE_STALLS.RS:sa=2000003,RESOURCE_STALLS.SB:sa=2000003,UOPS_ISSUED.ANY:sa=2000003,UOPS_ISSUED.CORE_STALL_CYCLES:sa=2000003,UOPS_RETIRED.ALL_PS:sa=2000003,UOPS_RETIRED.RETIRE_SLOTS_PS:sa=2000003,OFFCORE_RESPONSE.PF_LLC_DATA_RD.LLC_HIT.ANY_RESPONSE_0:sa=100003,OFFCORE_RESPONSE.PF_LLC_DATA_RD.LLC_MISS.ANY_RESPONSE_0:sa=100003 -data-limit=1024 $searchdir -duration=20  --target-pid $r -result-dir $2/custom >> $2/profile2.txt 
	;;
	6)	#context switch
		amplxe-cl -collect advanced-hotspots -knob collection-detail=stack-sampling -data-limit=0 $searchdir --start-paused --resume-after 10 --target-pid  $r -result-dir $outputPath/context >> $2/profile6.txt;;
	4)	#Remote memory
		 amplxe-cl -collect hpc-performance -data-limit=1024 --target-pid $r -result-dir $2/hpc >> $2/profile4.txt;;
	5)	#intel PMU
		toplev.py -l3 sleep 10
		;;
	3) 	#ocperf
		profile_RMA.sh $outputPath/profile3.txt $profile_duration
		;;
	7)	#IMC
		perf stat -e -a -per-socket  uncore_imc_0/event=0x4,umask=0x3/,uncore_imc_1/event=0x4,umask=0x3/,uncore_imc_4/event=0x4,umask=0x3/,uncore_imc_5/event=0x4,umask=0x3/
		;;
	8)	#ocperf PID
		 ./profile_RMA_PID.sh $outputPath/profile8.txt $r
		;;
	9)	#ocperf PID LLC
		 ./profile_LLC_PID.sh $outputPath/profile9.txt $r
	esac
}

#$hz $app 0 50 $tt
function statistics_profile {
# echo 3 | sudo tee /proc/sys/vm/drop_caches
killall -9 java
	arg_benchmark=" -st 1 -sit 1 -tt $5 --num_socket 8 --num_cpu 18 --THz 500000 --runtime 5000 --loop 100000 --size_tuple $3 --repeat 1 -bt $bt
	 --percentile $4"
	gc_thread=$tt #$((288)) #$1*
	echo "heap_size: $heap_size gc_threads: $gc_thread"
	gc_flag="-Xms1024g -Xmx1024g -XX:+UseParallelGC -XX:ParallelGCThreads=18 " #-XX:+UseNUMA -XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode -XX:ParallelCMSThreads=${gc_thread}
	JVM_args_profile="$gc_flag -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 -javaagent:$HOME/Documents/briskstream/common/lib/classmexer.jar " #  -XcompilationThreads=1
	#######Application profiling
	echo "=============== profiling phase:" $arg_benchmark "==================" #-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005
	numactl --localalloc -N 0,1 java $JVM_args_profile -jar $JAR_PATH $arg_benchmark -mp $outputPath --profile -a $2 --machine $machine >> profile\_$2\_$bt\_$5.txt
	cat profile\_$2\_$bt\_$5.txt
	#cat DUMP.txt
}

#$1:num_sockets $2:num_cpus
function execution {
#require: $argument $path $input $bt $Profile $arg_application $app $machine $num_socket $num_cpu $hz
        echo "$argument $arg_benchmark $arg_application --percentile $percentile"

        #-XX:+UseCondCardMark
        #Enables checking of whether the card is already marked before updating the card table.
        #This option is disabled by default and should only be used on machines with multiple sockets,
        #where it will increase performance of Java applications that rely heavily on concurrent operations.
        #Only the Java HotSpot Server VM supports this option.

        gc_thread=$tt #$((288)) #$1*
        heap_size=$((50)) ##$1*
        young_size=$(($heap_size/2))
        echo "heap_size: $heap_size gc_threads: $gc_thread"
        gc_flag="-Xms$heap_sizeg -Xmx$heap_sizeg" #-XX:ParallelGCThreads=$gc_thread #-XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode -XX:ParallelCMSThreads=${gc_thread}
        GC_args="-Xloggc:$path-gc.log -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintTenuringDistribution -XX:+PrintGCCause -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=5M"
        JVM_args1="$gc_flag"  # -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005  -XX:ParallelGCThreads  -XX:+UseG1GC -Xmn${young_size}g -XX:+UseCondCardMark
        JVM_args2="-agentpath:/home/shuhao/jprofiler10/bin/linux-x64/libjprofilerti.so=port=8848 -XX:CompileThreshold=100 -Xms4096g -Xmx4096g -XX:+UseG1GC -javaagent:$HOME/Documents/briskstream/common/lib/classmexer.jar"

        #Unfortuntly, I have no sudo right to use ``nice".
		if [ $Profile != 0 ] ; then
			java ${JVM_args1} -jar $JAR_PATH $arg_benchmark  $arg_application  --percentile $percentile  >> $path/test\_$iteration\_$bt\_$gc_factor.txt		&
		    profile $profile_type $path
		    #amplxe-cl -collect general-exploration -knob collect-memory-bandwidth=true -target-duration-type=medium -duration=500 $searchdir  --start-paused --resume-after 100000 -result-dir $path/resource java ${JVM_args1} -jar $JAR_PATH $arg_benchmark  $arg_application --percentile $percentile >> $path/test\_$iteration\_$bt\_$gc_factor.txt
		else #
		    case "$1" in
		        1) numactl --localalloc -N 1 java ${JVM_args1} -jar $JAR_PATH $arg_benchmark $arg_application  --percentile $percentile  >> $path/test\_$iteration\_$bt\_$gc_factor\_$CCOption.txt
		        ;;
		        2) numactl --localalloc -N 1,2 java ${JVM_args1} -jar $JAR_PATH $arg_benchmark $arg_application  --percentile $percentile  >> $path/test\_$iteration\_$bt\_$gc_factor\_$CCOption.txt
                ;;
                3) numactl --localalloc -N 1,2,3 java ${JVM_args1} -jar $JAR_PATH $arg_benchmark $arg_application  --percentile $percentile  >> $path/test\_$iteration\_$bt\_$gc_factor\_$CCOption.txt
                ;;
                4) numactl --localalloc -N 1,2,3,4 java ${JVM_args1} -jar $JAR_PATH $arg_benchmark $arg_application  --percentile $percentile  >> $path/test\_$iteration\_$bt\_$gc_factor\_$CCOption.txt
                ;;
                5) numactl --localalloc -N 1,2,3,4,5 java ${JVM_args1} -jar $JAR_PATH $arg_benchmark $arg_application  --percentile $percentile  >> $path/test\_$iteration\_$bt\_$gc_factor\_$CCOption.txt
                ;;
                6) numactl --localalloc -N 1,2,3,4,5,6 java ${JVM_args1} -jar $JAR_PATH $arg_benchmark $arg_application  --percentile $percentile  >> $path/test\_$iteration\_$bt\_$gc_factor\_$CCOption.txt
                ;;
                7) numactl --localalloc -N 1,2,3,4,5,6,7 java ${JVM_args1} -jar $JAR_PATH $arg_benchmark $arg_application  --percentile $percentile  >> $path/test\_$iteration\_$bt\_$gc_factor\_$CCOption.txt
                ;;
                8) numactl --localalloc java ${JVM_args1} -jar $JAR_PATH $arg_benchmark $arg_application  --percentile $percentile  >> $path/test\_$iteration\_$bt\_$gc_factor\_$CCOption.txt
                ;;

		    esac
		fi

        cat $path/test\_$iteration\_$bt\_$gc_factor\_$CCOption.txt | grep "finished measurement (k events/s)"
        cat $path/test\_$iteration\_$bt\_$gc_factor\_$CCOption.txt | grep "predict throughput (k events/s)"
        cat $path/test\_$iteration\_$bt\_$gc_factor\_$CCOption.txt | grep "Bounded throughput (k events/s)"

        cat $path/test\_$iteration\_$bt\_$gc_factor\_$CCOption.txt | grep "finished measurement (k events/s)" >> $outputPath/test_aware_throughput\_$hz\_$1\_$2\_$percentile\_$bt\_$gc_factor\_$CCOption.txt
        cat $path/test\_$iteration\_$bt\_$gc_factor\_$CCOption.txt | grep "predict throughput (k events/s)" >> $outputPath/test_aware_model\_$hz\_$1\_$2\_$percentile\_$bt\_$gc_factor\_$CCOption.txt
        cat $path/test\_$iteration\_$bt\_$gc_factor\_$CCOption.txt | grep "Bounded throughput (k events/s)" >> $outputPath/test_aware_bounded\_$hz\_$1\_$2\_$percentile\_$bt\_$gc_factor\_$CCOption.txt
}

# $4 $5 $path $gc_factor $tt
function sim_execution {
#require: $argument $path $input $bt $Profile $arg_application $app $machine $num_socket $num_cpu $hz
        # echo "streaming phase:" $argument >> $path/test\_$input\_$bt.txt

        JVM_args_sim="-javaagent:$HOME/Documents/briskstream/common/lib/classmexer.jar"
		if [ $Profile == 1 ] ; then
			java $JVM_args_sim -jar $JAR_PATH $arg_benchmark  $arg_application >> $path/test\_$iteration\_$bt\_$gc_factor\_$CCOption\_$theta.txt		&
			profile $profile_type $path
		else
			java $JVM_args_sim -jar $JAR_PATH $arg_benchmark  $arg_application>>  $path/test\_$iteration\_$bt\_$gc_factor\_$CCOption\_$theta.txt
		fi

        cat $path/test\_$iteration\_$bt\_$gc_factor\_$CCOption\_$theta.txt | grep "predict throughput (k events/s)"
        cat $path/test\_$iteration\_$bt\_$gc_factor\_$CCOption\_$theta.txt | grep "Bounded throughput (k events/s)"
}

#main_toff $Profile $hz $app 8 -1 $tt $input $bt
function local_execution {
        #require: $argument $path $input $bt $Profile $arg_application $app $machine $num_socket $num_cpu $hz
        # echo "streaming phase:" $argument >> $path/test\_$input\_$bt.txt

        JVM_args_local="-Xms50g -Xmx50g -XX:ParallelGCThreads=$tt -XX:CICompilerCount=2" # -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 -XX:ParallelGCThreads=$tt -XX:CICompilerCount=2

		if [ $Profile == 1 ] ; then
			 java $JVM_args_local -jar $JAR_PATH $arg_benchmark  $arg_application >> $path/test\_$iteration\_$bt\_$gc_factor\_$CCOption\_$theta.txt		&
			profile $profile_type $path
		else
			 java $JVM_args_local -jar $JAR_PATH $arg_benchmark  $arg_application>> $path/test\_$iteration\_$bt\_$gc_factor\_$CCOption\_$theta.txt
		fi

        cat $path/test\_$iteration\_$bt\_$gc_factor\_$CCOption\_$theta.txt | grep "finished measurement (k events/s)"
        cat $path/test\_$iteration\_$bt\_$gc_factor\_$CCOption\_$theta.txt | grep "finished measurement (k events/s)" >> $outputPath/test_aware_throughput\_$hz\_$1\_$2\_$percentile\_$bt\_$gc_factor\_$CCOption\_$theta.txt
}


#$Profile $hz $pt $ct1 $ct2 $app --num_socket $7 --num_cpu
function main_aware {
# echo 3 | sudo tee /proc/sys/vm/drop_caches
killall -9 java
		argument="application: $3 num_socket: $4 num_cpu: $5 hz: $2 total threads: $6  --machine $machine "
		# echo $argument
		arg_benchmark="--THz $2 --runtime 150  --loop 1000 --num_socket $4 --num_cpu $5  --size_tuple 256 "
		arg_application="--compressRatio 5
		 -st 5 -sit 10 -tt $6 -input $iteration -bt $bt --relax 1 -a $app -mp $outputPath/aware/$hz " #   --random  --worst

		#####Planed execution
		echo "=============== aware phase:" $argument $arg_benchmark $arg_application "=============="
		path=$outputPath/aware/$2/$4\_$6
		mkdir -p $path
		execution  $4 $5
}

#$Profile $hz $pt $ct1 $ct2 $app --num_socket $7 --num_cpu
function main_random_tune {
# echo 3 | sudo tee /proc/sys/vm/drop_caches
killall -9 java
		argument="application: $3 num_socket: $4 num_cpu: $5 hz: $2 total threads: $6  --machine $machine "
		# echo $argument
		arg_benchmark="--compressRatio -1 --parallelism_tune --THz $2 --runtime 150  --loop 1000 --num_socket $4 --num_cpu $5  --size_tuple 256 "
		arg_application="-st 1 -sit 10 -tt $6 -input $iteration -bt $bt --random  --relax 1 -a $app -mp $outputPath/random/$hz " # --tune  --random  --worst

		#####Planed execution
		echo "=============== random phase:" $argument $arg_benchmark $arg_application"=============="
		path=$outputPath/random/$2/$4\_$6
		mkdir -p $path
		execution $4 $5
}

#$Profile $hz $pt $ct1 $ct2 $app --num_socket $7 --num_cpu
function main_random {
# echo 3 | sudo tee /proc/sys/vm/drop_caches
killall -9 java
		argument="application: $3 num_socket: $4 num_cpu: $5 hz: $2 total threads: $6  --machine $machine "
		# echo $argument
		arg_benchmark="--THz $2 --runtime 150  --loop 1000 --num_socket $4 --num_cpu $5  --size_tuple 256 "
		arg_application="-st 5 -sit 5 -tt $6 -input $iteration -bt $bt --random  --relax 1 -a $app -mp $outputPath/random/$hz " # --tune  --random  --worst
		
		#####Planed execution
		echo "=============== random phase:" $argument $arg_benchmark $arg_application"=============="
		path=$outputPath/random/$2/$4\_$6
		mkdir -p $path
		execution $4 $5
}

#$Profile $hz $pt $ct1 $ct2 $app --num_socket $7 --num_cpu
function main_load_random {
# echo 3 | sudo tee /proc/sys/vm/drop_caches
killall -9 java
		argument="application: $3 num_socket: $4 num_cpu: $5 hz: $2 total threads: $6  --machine $machine "
		arg_benchmark="--THz $2 --runtime 1000  --loop 1000 --num_socket $4 --num_cpu $5  --size_tuple 256 "
		arg_application="-st 5 -sit 10 -tt $6 -input $iteration -bt $bt --parallelism_tune --relax 1 -a $app -mp $outputPath/opt/$hz --load --random" #   --random  --worst

		#####Planed execution
		echo "=============== load random phase:" $argument $arg_benchmark $arg_application"=============="
		path=$outputPath/opt/$hz/$4\_$6
		mkdir -p $path
        execution  $4 $5
}

#$Profile $hz $pt $ct1 $ct2 $app --num_socket $7 --num_cpu
function main_toff {
# echo 3 | sudo tee /proc/sys/vm/drop_caches
killall -9 java
		argument="application: $3 num_socket: $4 num_cpu: $5 hz: $2 total threads: $6  --machine $machine "
		# echo $argument
		arg_benchmark="--THz $2 --runtime 150  --loop 1000 --num_socket $4 --num_cpu $5  --size_tuple 256 "
		arg_application="-st 5 -sit 10 -tt $6 -input $iteration -bt $bt --toff  --relax 1 -a $app -mp $outputPath/toff/$hz " #   --random  --worst

		#####Planed execution
		echo "=============== ff phase:" $argument $arg_benchmark $arg_application"=============="
		path=$outputPath/toff/$2/$4\_$6
		mkdir -p $path
        execution     $4 $5
}

#$Profile $hz $pt $ct1 $ct2 $app --num_socket $7 --num_cpu
function main_toff_tune {
# echo 3 | sudo tee /proc/sys/vm/drop_caches
killall -9 java
		argument="application: $3 num_socket: $4 num_cpu: $5 hz: $2 total threads: $6  --machine $machine "
		# echo $argument
		arg_benchmark="--compressRatio -1 --parallelism_tune --THz $2 --runtime 150  --loop 1000 --num_socket $4 --num_cpu $5  --size_tuple 256 "
		arg_application="-st 5 -sit 10 -tt $6 -input $iteration -bt $bt --toff  --relax 1 -a $app -mp $outputPath/toff/$hz " #   --random  --worst

		#####Planed execution
		echo "=============== ff phase:" $argument $arg_benchmark $arg_application"=============="
		path=$outputPath/toff/$2/$4\_$6
		mkdir -p $path
        execution     $4 $5
}

#$Profile $hz $pt $ct1 $ct2 $app --num_socket $7 --num_cpu
function main_roundrobin_tune {
# echo 3 | sudo tee /proc/sys/vm/drop_caches
killall -9 java
		argument="application: $3 num_socket: $4 num_cpu: $5 hz: $2 total threads: $6  --machine $machine "
		# echo $argument
		arg_benchmark="--compressRatio -1 --parallelism_tune --THz $2 --runtime 150  --loop 1000 --num_socket $4 --num_cpu $5  --size_tuple 256 "
		arg_application="-st 5 -sit 10 -tt $6 -input $iteration -bt $bt --roundrobin  --relax 1 -a $app -mp $outputPath/roundrobin/$hz " #   --random  --worst

		#####Planed execution
		echo "=============== roundrobin phase:" $argument $arg_benchmark $arg_application"=============="
		path=$outputPath/roundrobin/$2/$4\_$6
		mkdir -p $path
        execution      $4 $5
}

#$Profile $hz $pt $ct1 $ct2 $app --num_socket $7 --num_cpu
function main_roundrobin {
# echo 3 | sudo tee /proc/sys/vm/drop_caches
killall -9 java
		argument="application: $3 num_socket: $4 num_cpu: $5 hz: $2 total threads: $6  --machine $machine "
		# echo $argument
		arg_benchmark="--THz $2 --runtime 150  --loop 1000 --num_socket $4 --num_cpu $5  --size_tuple 256 "
		arg_application="-st 5 -sit 10 -tt $6 -input $iteration -bt $bt --roundrobin  --relax 1 -a $app -mp $outputPath/roundrobin/$hz " #   --random  --worst
		
		#####Planed execution
		echo "=============== roundrobin phase:" $argument $arg_benchmark $arg_application"=============="
		path=$outputPath/roundrobin/$2/$4\_$6
		mkdir -p $path
        execution      $4 $5       
}


#$Profile $hz $pt $ct1 $ct2 $app --num_socket $7 --num_cpu
function main_opt_ft {
# echo 3 | sudo tee /proc/sys/vm/drop_caches
killall -9 java
		argument="application: $3 num_socket: $4 num_cpu: $5 hz: $2 total threads: $6  --machine $machine "
		arg_benchmark="--THz $2 --runtime 250 --loop 1000 --num_socket $4 --num_cpu $5  --size_tuple 256 "
		arg_application="--checkpoint $ck --Fault_tolerance --compressRatio -1 -st 5 -sit 10 -tt $6 -input $iteration -bt $bt --parallelism_tune --relax 1 -a $app -mp $outputPath/opt/$percentile/$hz " #   --random  --worst

		#####Planed execution
		echo "=============== opt phase =============="
		path=$outputPath/opt/$percentile/$hz/$4\_$6
		mkdir -p $path
        execution  $4 $5
}


#$Profile $hz $pt $ct1 $ct2 $app --num_socket $7 --num_cpu
function main_opt {
# echo 3 | sudo tee /proc/sys/vm/drop_caches
killall -9 java
		argument="application: $3 num_socket: $4 num_cpu: $5 hz: $2 total threads: $6  --machine $machine "
		arg_benchmark="--THz $2 --runtime 30  --loop 1000 --num_socket $4 --num_cpu $5  --size_tuple 256 "
		arg_application="--gc_factor $gc_factor --backPressure --compressRatio -1 --parallelism_tune -st 1 -sit 1 -tt $6 -input $iteration -bt $bt --relax 1 -a $app -mp $outputPath/opt/$percentile " #   --random  --worst
		
		#####Planed execution
		echo "=============== opt phase =============="
		path=$outputPath/opt/$percentile/$4\_$6
		mkdir -p $path
        execution $4 $5 $path $gc_factor $tt
}

#$Profile $hz $pt $ct1 $ct2 $app --num_socket $7 --num_cpu
function main_opt_sim {
# echo 3 | sudo tee /proc/sys/vm/drop_caches
        path=$outputPath/opt/$percentile/$hz/$4\_$6
		argument="application: $3 num_socket: $4 num_cpu: $5 hz: $2 total threads: $6  --machine $machine "
		arg_benchmark="--THz $2 --runtime 100  --loop 1000 --num_socket $4 --num_cpu $5  --size_tuple 256 "
		arg_application="--gc_factor $gc_factor --backPressure  --simulation --compressRatio $r -st 5 -sit 10 -tt $6 -input $iteration -bt $bt --parallelism_tune --relax 1 -a $app -mp $outputPath/opt/$percentile " #   --random  --worst

		#####Planed execution
		echo "=============== opt phase =============="
		mkdir -p $path
        sim_execution   $4 $5 $path $gc_factor $tt
}

#$Profile $hz $pt $ct1 $ct2 $app --num_socket $7 --num_cpu
function main_load_opt {
# echo 3 | sudo tee /proc/sys/vm/drop_caches
killall -9 java
		argument="application: $3 num_socket: $4 num_cpu: $5 hz: $2 total threads: $6  --machine $machine "
		arg_benchmark="--THz $2 --runtime 600  --loop 1000 --num_socket $4 --num_cpu $5  --size_tuple 256 "
		arg_application="--gc_factor $gc_factor -st 5 -sit 10 -tt $6 -input $iteration -bt $bt --relax 1 -a $app -mp $outputPath/opt/$percentile --load --parallelism_tune" #   --random  --worst

		#####Planed execution
		echo "=============== load opt phase:" $argument $arg_benchmark $arg_application"=============="
		path=$outputPath/opt/$percentile/$4\_$6
		mkdir -p $path
        execution  $4 $5
}

#$Profile $hz $pt $ct1 $ct2 $app --num_socket $7 --num_cpu
function main_load {
# echo 3 | sudo tee /proc/sys/vm/drop_caches
killall -9 java
		argument="application: $3 num_socket: $4 num_cpu: $5 hz: $2 total threads: $6  --machine $machine "
		arg_benchmark="--THz $2 --runtime 250  --loop 1000 --num_socket $4 --num_cpu $5  --size_tuple 256 "
		arg_application="-st 5 -sit 10 -tt $6 -input $iteration -bt $bt --relax 1 -a $app -mp $outputPath/opt/$percentile --load " #   --random  --worst
		
		#####Planed execution
		echo "=============== load opt phase:" $argument $arg_benchmark $arg_application"=============="
		path=$outputPath/opt/$hz/$4\_$6
		mkdir -p $path
        execution  $4 $5 
}


#$Profile $hz $pt $ct1 $ct2 $app --num_socket $7 --num_cpu
function main_load_ft {
# echo 3 | sudo tee /proc/sys/vm/drop_caches
killall -9 java
		argument="application: $3 num_socket: $4 num_cpu: $5 hz: $2 total threads: $6  --machine $machine "
		arg_benchmark="--THz $2 --runtime 1000  --loop 1000 --num_socket $4 --num_cpu $5  --size_tuple 256 "
		arg_application="--checkpoint $ck --Fault_tolerance -st 5 -sit 10 -tt $6 -input $iteration -bt $bt --parallelism_tune --relax 1 -a $app -mp $outputPath/opt --load " #   --random  --worst

		#####Planed execution
		echo "=============== load opt phase:" $argument $arg_benchmark $arg_application"=============="
		path=$outputPath/opt/$hz/$4\_$6
		mkdir -p $path
        execution  $4 $5
}


#$Profile $hz $pt $ct1 $ct2 $app --num_socket $7 --num_cpu
function main_aware_worst {
# echo 3 | sudo tee /proc/sys/vm/drop_caches
killall -9 java
		argument="application: $3 num_socket: $4 num_cpu: $5 hz: $2 total threads: $6  --machine $machine "
		# echo $argument
		arg_benchmark="--THz $2 --runtime 90  --loop 1000 --num_socket $4 --num_cpu $5  --size_tuple 256 "
		arg_application="-st 5 -sit 10 -tt $6 -input $iteration -bt $bt --random --worst  --relax 1 -a $app -mp $outputPath/worst " # --tune  --random  --worst
		
		#####Planed execution
		echo "=============== worst phase:" $argument $arg_benchmark $arg_application"=============="
		path=$outputPath/worst/$2/$4\_$6
		mkdir -p $path
        execution    $4 $5               
}


#$Profile $hz $pt $ct1 $ct2 $app --num_socket $7 --num_cpu
function main_native_tune {
# echo 3 | sudo tee /proc/sys/vm/drop_caches
killall -9 java
		argument="application: $3 num_socket: $4 num_cpu: $5 hz: $2 total threads: $6  --machine $machine "
		# echo $argument
		arg_benchmark="--compressRatio -1 --parallelism_tune --THz $2 --runtime 200  --loop 1000 --num_socket $4 --num_cpu $5  --size_tuple 256 "
		arg_application="-st 5 -sit 10 -tt $6 -input $iteration -bt $bt --native --relax 1 -a $app -mp $outputPath/native/$hz " # --tune  --random  --worst

		#####native execution
		echo "=============== native phase:" $argument $arg_benchmark $arg_application"=============="
		path=$outputPath/native/$2/$4\_$6
		mkdir -p $path
        execution   $4 $5
}

#$Profile $hz $pt $ct1 $ct2 $app --num_socket $7 --num_cpu
function main_native {
# echo 3 | sudo tee /proc/sys/vm/drop_caches
killall -9 java
		argument="application: $3 num_socket: $4 num_cpu: $5 hz: $2 total threads: $6  --machine $machine "
		# echo $argument
		arg_benchmark="--THz $2 --runtime 100  --loop 1000 --num_socket $4 --num_cpu $5  --size_tuple 256 "
		arg_application="-st 1 -sit 1 -tt $tt --TP $TP -input $iteration -bt $bt --native --relax 1 -a $app -mp $outputPath/native/$hz " # --tune  --random  --worst
		
		#####native execution
		echo "=============== native phase:" $argument $arg_benchmark $arg_application"=============="
		path=$outputPath/native/$2/$4\_$6
		mkdir -p $path
        execution   $4 $5 
}

#$Profile $hz $pt $ct1 $ct2 $app --num_socket $7 --num_cpu
function main_transaction_native {
# echo 3 | sudo tee /proc/sys/vm/drop_caches
killall -9 java
        path=$outputPath/native/$2/$4\_$6\_$CCOption\_$theta
		argument="application: $3 num_socket: $4 num_cpu: $5 hz: $2 total threads: $6  --machine $machine "
		# echo $argument
		arg_benchmark="--THz $2 --runtime 10  --loop 1000 --num_socket $4 --num_cpu $5  --size_tuple 256 --transaction --TP $TP --CCOption $CCOption --checkpoint $checkpoint --theta $theta --NUM_ACCESS $NUM_ACCESS --ratio_of_read $ratio_of_read"
		arg_application="-st $st -sit 1 -tt $tt -input $iteration -bt $bt --native --relax 1 -a $app -mp $path " # --tune  --random  --worst

		#####native execution
		echo "=============== native phase:" $argument $arg_benchmark $arg_application"=============="
		mkdir -p $path
        execution $4 $5
}


#$Profile $hz $pt $ct1 $ct2 $app --num_socket $7 --num_cpu
function main_transaction_native_local {
# echo 3 | sudo tee /proc/sys/vm/drop_caches
        path=$outputPath/native/$2/$4\_$6\_$CCOption\_$theta\_$NUM_ACCESS\_$ratio_of_read
		argument="application: $3 num_socket: $4 num_cpu: $5 hz: $2 total threads: $6  --machine $machine"
		# echo $argument
		arg_benchmark="--THz $2 --runtime 10  --loop 1000 --num_socket $4 --num_cpu $5  --size_tuple 256 --transaction --TP $TP --CCOption $CCOption --checkpoint $checkpoint --theta $theta --NUM_ACCESS $NUM_ACCESS --ratio_of_read $ratio_of_read"
		arg_application="-st $st -sit 1 -tt $tt -input $iteration -bt $bt --native --relax 1 -a $app -mp $path" #--measure --tune  --random  --worst

		#####native execution
		echo "=============== native phase:" $argument $arg_benchmark $arg_application"=============="
		mkdir -p $path
        local_execution $4 $5 $tt
}


#$Profile $hz $pt $ct1 $ct2 $app --num_socket $7 --num_cpu
function main_native_simple {
# echo 3 | sudo tee /proc/sys/vm/drop_caches
#killall -9 java
		argument="application: $3 num_socket: $4 num_cpu: $5 hz: $2 total threads: $6  --machine $machine "
		# echo $argument
		arg_benchmark="--THz $2 --runtime 15  --loop 1000 --num_socket $4 --num_cpu $5  --size_tuple 256 "
		arg_application="-st 1 -sit 2 -tt $6 -input $iteration -bt $bt --native --relax 1 -a $app -mp $outputPath/native/$hz " # --tune  --random  --worst

		#####native executionm
		echo "=============== native phase:" $argument $arg_benchmark $arg_application"=============="
		path=$outputPath/native/$2/$4\_$6
		mkdir -p $path
        local_execution   $4 $5
}

#$Profile $hz $pt $ct1 $ct2 $app --num_socket $7 --num_cpu
function main_native_ft {
# echo 3 | sudo tee /proc/sys/vm/drop_caches
killall -9 java
		argument="application: $3 num_socket: $4 num_cpu: $5 hz: $2 total threads: $6  --machine $machine "
		# echo $argument
		arg_benchmark="--THz $2 --runtime 30  --loop 1000 --num_socket $4 --num_cpu $5  --size_tuple 256 "
		arg_application="--checkpoint $ck --Fault_tolerance -st 5 -sit 10 -tt $6 -input $iteration -bt $bt --native --relax 1 -a $app -mp $outputPath/native/$hz " # --tune  --random  --worst

		#####native execution
		echo "=============== native phase:" $argument $arg_benchmark $arg_application"=============="
		path=$outputPath/native/$2/$4\_$6\_$ck
		mkdir -p $path
        execution   $4 $5
}

#$Profile $hz $pt $ct1 $ct2 $app --num_socket $7 --num_cpu
function main_native_Linked {
# echo 3 | sudo tee /proc/sys/vm/drop_caches
killall -9 java
		argument="application: $3 num_socket: $4 num_cpu: $5 hz: $2 total threads: $6  --machine $machine "
		# echo $argument
		arg_benchmark="--linked --THz $2 --runtime 200  --loop 1000 --num_socket $4 --num_cpu $5  --size_tuple 256 "
		arg_application="-st 5 -sit 10 -tt $6 -input $iteration -bt $bt --native --relax 1 -a $app -mp $outputPath/native_spsc_linked/$hz " # --tune  --random  --worst

		#####native execution
		echo "=============== native_linked phase:" $argument $arg_benchmark $arg_application"=============="
		path=$outputPath/native_spsc_linked/$2/$4\_$6
		mkdir -p $path
        execution $4 $5
}

#$Profile $hz $pt $ct1 $ct2 $app --num_socket $7 --num_cpu
function main_native_MPSC {
# echo 3 | sudo tee /proc/sys/vm/drop_caches
killall -9 java
		argument="application: $3 num_socket: $4 num_cpu: $5 hz: $2 total threads: $6  --machine $machine "
		# echo $argument
		arg_benchmark="--shared --THz $2 --runtime 200  --loop 1000 --num_socket $4 --num_cpu $5  --size_tuple 256 "
		arg_application="-st 5 -sit 10 -tt $6 -input $iteration -bt $bt --native --relax 1 -a $app -mp $outputPath/native_mpsc/$hz " # --tune  --random  --worst

		#####native execution
		echo "=============== native_mpsc phase:" $argument $arg_benchmark $arg_application"=============="
		path=$outputPath/native_mpsc/$2/$4\_$6
		mkdir -p $path
        execution   $4 $5
}
	#$Profile $hz $pt $ct1 $ct2 $app --num_socket $7 --num_cpu
function main_native_MPSC_Linked {
# echo 3 | sudo tee /proc/sys/vm/drop_caches
killall -9 java
		argument="application: $3 num_socket: $4 num_cpu: $5 hz: $2 total threads: $6  --machine $machine "
		# echo $argument
		arg_benchmark="--linked --shared --THz $2 --runtime 200  --loop 1000 --num_socket $4 --num_cpu $5  --size_tuple 256 "
		arg_application="-st 5 -sit 10 -tt $6 -input $iteration -bt $bt --native --relax 1 -a $app -mp $outputPath/native_mpsc_linked
		/$hz " # --tune  --random  --worst

		#####native execution
		echo "=============== native_mpsc_linked phase:" $argument $arg_benchmark $arg_application"=============="
		path=$outputPath/native_mpsc_linked/$2/$4\_$6
		mkdir -p $path
        execution   $4 $5
}


#$Profile $hz $pt $ct1 $ct2 $app --num_socket $7 --num_cpu
function main_native_SPMC {
# echo 3 | sudo tee /proc/sys/vm/drop_caches
killall -9 java
		argument="application: $3 num_socket: $4 num_cpu: $5 hz: $2 total threads: $6  --machine $machine "
		# echo $argument
		arg_benchmark="--common --THz $2 --runtime 200  --loop 1000 --num_socket $4 --num_cpu $5  --size_tuple 256 "
		arg_application="-st 5 -sit 10 -tt $6 -input $iteration -bt $bt --native --relax 1 -a $app -mp $outputPath/native_spmc/$hz " # --tune  --random  --worst

		#####native execution
		echo "=============== native_spmc phase:" $argument $arg_benchmark $arg_application"=============="
		path=$outputPath/native_spmc/$2/$4\_$6
		mkdir -p $path
        execution   $4 $5
}

#$Profile $hz $pt $ct1 $ct2 $app --num_socket $7 --num_cpu
function main_native_SPMC_Linked {
# echo 3 | sudo tee /proc/sys/vm/drop_caches
killall -9 java
		argument="application: $3 num_socket: $4 num_cpu: $5 hz: $2 total threads: $6  --machine $machine "
		# echo $argument
		arg_benchmark="--common --linked --THz $2 --runtime 200  --loop 1000 --num_socket $4 --num_cpu $5  --size_tuple 256 "
		arg_application="-st 5 -sit 10 -tt $6 -input $iteration -bt $bt --native --relax 1 -a $app -mp $outputPath/native_spmc_linked/$hz " # --tune  --random  --worst

		#####native execution
		echo "=============== native_spmc_linked phase:" $argument $arg_benchmark $arg_application"=============="
		path=$outputPath/native_spmc_linked/$2/$4\_$6
		mkdir -p $path
        execution   $4 $5
}


#$Profile $hz $pt $ct1 $ct2 $app --num_socket $7 --num_cpu
function main_native_MPMC {
# echo 3 | sudo tee /proc/sys/vm/drop_caches
killall -9 java
		argument="application: $3 num_socket: $4 num_cpu: $5 hz: $2 total threads: $6  --machine $machine "
		# echo $argument
		arg_benchmark="--shared --common --THz $2 --runtime 200  --loop 1000 --num_socket $4 --num_cpu $5  --size_tuple 256 "
		arg_application="-st 5 -sit 10 -tt $6 -input $iteration -bt $bt --native --relax 1 -a $app -mp $outputPath/native_mpmc/$hz " # --tune  --random  --worst

		#####native execution
		echo "=============== native_mpmc phase:" $argument $arg_benchmark $arg_application"=============="
		path=$outputPath/native_mpmc/$2/$4\_$6
		mkdir -p $path
        execution   $4 $5
}

#$Profile $hz $pt $ct1 $ct2 $app --num_socket $7 --num_cpu
function main_native_MPMC_Linked {
# echo 3 | sudo tee /proc/sys/vm/drop_caches
killall -9 java
		argument="application: $3 num_socket: $4 num_cpu: $5 hz: $2 total threads: $6  --machine $machine "
		# echo $argument
		arg_benchmark="--shared --common --linked --THz $2 --runtime 200  --loop 1000 --num_socket $4 --num_cpu $5  --size_tuple 256 "
		arg_application="-st 5 -sit 10 -tt $6 -input $iteration -bt $bt --native --relax 1 -a $app -mp $outputPath/native_mpmc_linked/$hz " # --tune  --random  --worst

		#####native execution
		echo "=============== native_mpmc_linked phase:" $argument $arg_benchmark $arg_application"=============="
		path=$outputPath/native_mpmc_linked/$2/$4\_$6
		mkdir -p $path
        execution   $4 $5
}


# Configurable variables
output=test.csv
# Generate a timestamp
timestamp=$(date +%Y%m%d-%H%M)
# Create a temporary directory
app_cnt=0
cnt=0
for app in "WordCount" #"FraudDetection" "SpikeDetection" "LogProcessing"  "LinearRoad"
do
    machine=3
    Profile=0 #vtune profile: 0 disable, 1 enable.
	profile_type=4 # 1 for general..4 for hpc.
	outputPath=$HOME/Documents/briskstream/Tests/test-$timestamp/$app
	mkdir -p $outputPath
	cd $outputPath
	# Save some system information
	uname -a > kernel.txt
	cat /proc/cpuinfo > cpuinfo.txt
	cat /proc/meminfo > meminfo.txt

	echo Benchmark initiated at $(date +%Y%m%d-%H%M)

	JAR_PATH="$HOME/Documents/briskstream/BriskBenchmarks/target/briskstream-1.2.0-jar-with-dependencies.jar"
    ##The below is for the case that #theads=15
	#SA=(5000	10000	15000	25000	 )
	# WC=(5000	10000	15000	25000	53416  )
	# FD=(5000	10000	15000   25000	86017  ) 
	# SD=(5000	10000	15000   25000	66974 )
	# TM=(5000	10000	15000   25000	38 )
	# LG=(5000	10000	15000   25000	41182 )
	# VS=(5000	10000	15000   25000	28817 )
	# LR=(5000	10000	15000   25000	46506 )
	
	#10 different input HZ.
	# SA=(150000) # 900 1800 2700 3600 4500 9000 18000 27000 36000 45000
	# WC=(53416 )
	# FD=(86017 ) 
	# SD=(66974 )
	# TM=(38 )
	# LG=(41182 )
	# VS=(28817 )
	# LR=(46506 )
    CT=(5000000)
    OB=(5000000)
    MB=(50000 100000 150000 150000 200000) #write can support only 100 K (100000). Read-only can support 500 k (300000).
    WC=(10000000)
	FD=(5000000 )
	SD=(5000000 )
	# TM=(450000 )
	LG=(5000000 )
	VS=(5000000 )
	LR=(5000000
	 )
    
	#5 repeats
    let "iteration = 1"
		case "$app" in
			"WordCount")
				for hz in "${WC[@]}"
				do
					echo "WordCount Study"

					for bt in 50
                    do
                        for percentile in 50 #90 99 # 100 #99 100 ##the percentile used in profiling..
                        do
                        END=18 #to get a ground-truth!!

                            for((tt=5;tt<=END;tt+=5));
                            do
                                statistics_profile $hz $app 0 $percentile $tt
                            done

                            for((tt=1;tt<2;tt+=2));
                            do
                                statistics_profile $hz $app 0 $percentile $tt
                            done

				        done #end of percentile

                        echo "scalability test"
                        for percentile in 50 #90 99 # 100 #99 100 ##the percentile used in profiling..
                        do
                            for socket in 7 #4 8 #2 3 4 5 6 7 8 #4 2 1
                            do
                                for cpu in 17 #18
                                do
                                    for gc_factor in 0 #5 10 20 30
                                    do
                                        echo "$socket"
                                        let "tt = 144/8*$socket"
                                        for r in -1
                                        do
                                            main_load_opt $Profile $hz $app $socket $cpu $tt iteration $bt $gc_factor $r
                                        done
                                    done
                                done
                            done
                        done #end of percentile

                        #native test.
#                        for socket in 8
#                        do
#                            for tt in 1
#                            do
##                                main_toff $Profile $hz $app $socket -1 $tt $input $bt
##                                main_roundrobin $Profile $hz $app $socket -1 $tt $input $bt
#                                 main_native $Profile $hz $app $socket -1 $tt $input $bt
#                            done
#                         done


                        #random placement test
#                        for iteration in 1 2 3 4 5 6 7 8 9 10
#                        do
#                            for socket in 8 #4 2 1
#                            do
#                                for percentile in 50 # 100 #99 100 ##the percentile used in profiling..
#                                do
#                                    #let "tt = 136/8*$socket"
#                                    let "tt = 1"
#                                    main_random $Profile $hz $app $socket 18 $tt $input $bt
#                                done
#                            done
#                        done #end of iteration
				    done #end of batch
				done #end of Hz
				;;
			"FraudDetection")
				for hz in "${FD[@]}"
				do
					echo "FraudDetection Study"
					for bt in 20
                    do
                        for percentile in 50 #90 99 # 100 #99 100 ##the percentile used in profiling..
                        do
                            END=18 #to get a ground-truth!!

#                            for((tt=5;tt<=END;tt+=5));
#                            do
#                                statistics_profile $hz $app 0 $percentile $tt
#                            done
#
#                            for((tt=1;tt<2;tt+=2));
#                            do
#                                statistics_profile $hz $app 0 $percentile $tt
#                            done

				        done #end of percentile

                        echo "scalability test"
                        for percentile in 50 #90 99 # 100 #99 100 ##the percentile used in profiling..
                        do
                            for socket in 7 #4 8 #2 3 4 5 6 7 8 #4 2 1
                            do
                                for cpu in 18 #18
                                do
                                    for gc_factor in 0 #5 10 20 30
                                    do
                                        echo "$socket"
                                        let "tt = 144/8*$socket"
                                        let "r = -1"
                                        main_opt_sim $Profile $hz $app $socket $cpu $tt iteration $bt $gc_factor $r
                                    done
                                done
                            done
                        done #end of percentile

#                        statistics_profile $hz $app 0 -1
#                        for socket in 8 #2 4 8 #4096 8 64 512
#                        do
#                             main_opt $Profile $hz $app 8 -1 1 $input $bt
#                        done
#
#                        for socket in 8 #4 2 1
#                        do
#                            for percentile in 99 # 100 #99 100 ##the percentile used in profiling..
#                            do
#                                let "tt = 288/8*$socket"
#
#                               main_toff_tune $Profile $hz $app 8 -1 $tt $iteration $bt
#                               main_roundrobin_tune $Profile $hz $app 8 -1 $tt $iteration $bt
#                              # main_aware $Profile $hz $app 8 -1 $tt $input $bt
#                               main_native_tune $Profile $hz $app 8 -1 $tt $iteration $bt
#                            done
#                        done
                    done
				done
				;;
			"SpikeDetection")
				for hz in "${SD[@]}"
				do
					echo "SpikeDetection Study"
					for bt in 10
                    do
                            for percentile in 50 #90 99 # 100 #99 100 ##the percentile used in profiling..
                            do
                                END=18 #to get a ground-truth!!
#
#                                for((tt=5;tt<=END;tt+=5));
#                                do
#                                    statistics_profile $hz $app 0 $percentile $tt
#                                done
#
#                                for((tt=1;tt<2;tt+=2));
#                                do
#                                    statistics_profile $hz $app 0 $percentile $tt
#                                done

                            done #end of percentile

                            echo "scalability test"
                            for percentile in 50 #90 99 # 100 #99 100 ##the percentile used in profiling..
                            do
                                for socket in 7 #4 8 #2 3 4 5 6 7 8 #4 2 1
                                do
                                    for cpu in 18 #18
                                    do
                                        for gc_factor in 0 #5 10 20 30
                                        do
                                            echo "$socket"
                                            let "r = -1"
                                            let "tt = 144/8*$socket"
                                            main_opt_sim $Profile $hz $app $socket $cpu $tt iteration $bt $gc_factor $r
                                        done
                                    done
                                done
                            done #end of percentile

                    done #end of bt
				done
				;;   
			# "TrafficMonitoring")
				# for hz in "${TM[@]}"
				# do
					# echo "TrafficMonitoring Study"
                    # for bt in 100                     
                    # do
                       # statistics_profile $hz $app 0 -1 0 0 $bt
                       # main_opt $Profile $hz $app 8 -1 -1 $input $bt 
                    # done                   
					# statistics_profile $hz $app 0 -1 0 0
					# for num_socket in 2 4 8
					# do
						# for cores in 8
						# do	
							# for pt in 1 #4 16
							# do                          
                                # for ct1 in 1 #16 8 4
                                # do
                                    # for ct2 in 1 #16 8 4
                                    # do
                                       
                                            # for tt in   1 
                                            # do
                                                # main_aware $Profile $hz $pt $ct1 $ct2 $app $num_socket $cores $tt $input
                                            # done 
                                       
                                    # done
                                # done
                            # done
						# done
					# done
                    # for tt in  5 25 45 65 85
                    # do
                        # main_aware $Profile $hz 1 1 1 $app 8 8 $tt $input
                        # main_native $Profile $hz 1 1 1 $app 8 8 $tt $input   
                    # done                        
				# done
				# ;; 		
#			"LogProcessing")
#				for hz in "${LG[@]}"
#				do
#					echo "Log Processing Study"
#					for bt in 10
#                    do
#                        #scalability test
#                        statistics_profile $hz $app 0 -1
#                        for socket in 8 4 2 1
#                        do
#                            echo "$socket"
##                            let "tt = 288/8*$socket"
##                            for percentile in 90 99 # 100 #99 100 ##the percentile used in profiling..
##                            do
##                                main_opt $Profile $hz $app $socket -1 $tt $input $bt
##                            done
#                        done
##                        statistics_profile $hz $app 0 -1
##                        for socket in 8 #2 4 8 #4096 8 64 512
##                        do
##                             main_opt $Profile $hz $app 8 -1 1 $input $bt
##                        done
##
##                        for tt in 288 #155 135 115 #15 25 35 45 90 135 #-1 5 15 25 35 45 55 65 75 85 95 105 135 165 185
##                        do
##                             main_toff $Profile $hz $app 8 -1 $tt $input $bt
##                             main_roundrobin $Profile $hz $app 8 -1 $tt $input $bt
##                             main_aware $Profile $hz $app 8 -1 $tt $input $bt
##                             main_native $Profile $hz $app 8 -1 $tt $input $bt
##                        done
#                    done
#				done
#				;;
			# "VoIPSTREAM")
				# for hz in "${VS[@]}"
				# do
					# echo "VoIPSTREAM Study"
                    # for bt in 10 100                     
                    # do
                       # statistics_profile $hz $app 0 -1 0 0 $bt
                       # main_opt $Profile $hz $app 8 -1 -1 $input $bt           
                       # for tt in 345 295 245 #15 25 35 45 90 135 #-1 5 15 25 35 45 55 65 75 85 95 105 135 165 185
                       # do
                           # main_native $Profile $hz $app 8 -1 $tt $input $bt
                           # main_aware $Profile $hz $app 8 -1 $tt $input $bt                           
                       # done   
                    # done	  
				# done
				# ;; 		
			"LinearRoad")
				for hz in "${LR[@]}"
				do  
                    echo "LinearRoad Study"
					for bt in 20
                    do
                        for percentile in 50 #90 99 # 100 #99 100 ##the percentile used in profiling..
                        do
                            END=20 #to get a ground-truth!!
#
#                            for((tt=1;tt<2;tt+=1));
#                            do
#                                statistics_profile $hz $app 0 $percentile $tt
#                            done
#
#                            for((tt=5;tt<=END;tt+=5));
#                            do
#                                statistics_profile $hz $app 0 $percentile $tt
#                            done

                        done #end of percentile

                        echo "scalability test"
                        for percentile in 50 #90 99 # 100 #99 100 ##the percentile used in profiling..
                        do
                            for socket in 7 #2 #4 8 #2 3 4 5 6 7 8 #4 2 1
                            do
                                for cpu in 18 #18
                                do
                                    for gc_factor in 0 #5 10 20 30
                                    do
                                        echo "$socket"
                                        let "r = -1"
                                        let "tt = 144/8*$socket"
                                        main_opt_sim $Profile $hz $app $socket $cpu $tt iteration $bt $gc_factor $r
                                    done
                                done
                            done
                        done #end of percentile

                        #native test.
#                        for socket in 8
#                        do
#                            for tt in 150 180 200
#                            do
##                                main_toff $Profile $hz $app $socket -1 $tt $input $bt
##                                main_roundrobin $Profile $hz $app $socket -1 $tt $input $bt
#                                 main_native $Profile $hz $app $socket -1 $tt $input $bt
#                            done
#                         done
#                        echo random placement test
#                        for iteration in 1 2 3 4 5 6 7 8 9 10
#                        do
#                            for socket in 7 #4 2 1
#                            do
#                                for percentile in 50 # 100 #99 100 ##the percentile used in profiling..
#                                do
#                                    let "tt = 136/8*$socket"
#                                    main_random_tune $Profile $hz $app $socket 18 $tt $input $bt
#                                done
#                            done
#                        done #end of iteration

                    done #end of bt
                  done
				;;
				"LinearRoad_latency")
				for hz in "${LR[@]}"
				do
                    echo "LinearRoad latency Study"
					for bt in 20
                    do
                       echo "latency test"
                        for percentile in 50 #90 99 # 100 #99 100 ##the percentile used in profiling..
                        do
                            for socket in 7 #4 2 1
                            do
                                for cpu in 18 #18
                                do
                                    for gc_factor in 0
                                    do
                                        echo "$socket"
                                        let "tt = 144/8*$socket"
                                        main_native $Profile $hz $app $socket $cpu $tt iteration $bt $gc_factor
                                    done
                                done
                            done
                        done #end of percentile

                    done #end of bt
                  done
				;;
			*)
				echo $"Usage: $0 {application}"
				exit 1
		esac
done #varing apps.
cd $HOME/scripts
./jobdone.py
