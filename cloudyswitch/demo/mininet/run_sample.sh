MN_PRE_FILE=/tmp/test-mn/mn-pre
MN_POST_FILE=/tmp/test-mn/mn-post
MN_FILE_DIR=/tmp/test-mn
# set default environment
set_env() {
    RYU_APP=../../app/switches_v1_3.py,../../app/switch_event_handlers.py
}
# making mininet-test-pre-file
mn_pre() {
    exec 3>&1
    exec >$MN_PRE_FILE
    echo "sh echo '(pre) tshark start.'"
    echo "sh echo '----------------------------------'"
    #If you want dump of packets
    #echo "$DUMP_HOST tshark -i $DUMP_IF -w $DUMP_FILE &"
    echo "sh echo '----------------------------------'"
    exec 1>&3
}

# making mininet-test-post-file
mn_post() {
    echo "mnPost"
    exec 3>&1
    exec >$MN_POST_FILE
    exec 1>&3
}

# ovs cache-hit incremental check
# starting ryu-manager
run_ryu() {
    ERRSTAT=0
    ERRTAG="run_ryu() :"
    export PYTHONPATH=$HOME/ryu
    echo "Inf: RYU_APP=$RYU_APP"
    echo "Inf: ryu-manager starting..."
    /home/mininet/ryu/bin/ryu-manager --verbose $RYU_APP &
    PID_RYU=$!
    sleep 1
    [ -d /proc/$PID_RYU ] || err $ERRTAG "failed to start ryu-manager."
    return $ERRSTAT
}

# starting mininet and test-script
run_mn() {
    echo "Info: mininet starting..."
    #sudo mn --pre $MN_PRE_FILE --post $MN_POST_FILE --switch user --controller remote --custom sample_topology.py --topo mytopo
    sudo python qos_test.py
}

# cleaning after mininet
clean_mn() {
    wait_ryu
    rm -f $MN_PRE_FILE $MN_POST_FILE
}

# stoping ryu-manager
wait_ryu() {
    kill -2 $PID_RYU
    wait $PID_RYU
}
[ -d $MN_FILE_DIR ] || mkdir -p $MN_FILE_DIR
# test-main
test_mn() {
    mn_pre
    mn_post
    run_ryu; [ $? -ne 0 ] && return 1
    run_mn; [ $? -ne 0 ] && return 1
    return 0
}

err() {
    echo Error: $*
    ERRSTAT=1
}

count=0
echo "¥n---------- test start ----------"
set_env
test_mn
clean_mn
echo "¥n---------- test finish ----------"

exit 0
