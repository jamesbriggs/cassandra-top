#!/bin/bash

# usage: ./tests.sh [host=localhost]

HOST=${1:-localhost}
HOST_NOT_LISTENING=${2:-}
HOST_NOT_REACHABLE="1.2.3.4"

program="./cass_top"

. assert.sh

assert_contains() {
    # assert_contains <str> <regex> [expected]
    (( tests_ran++ )) || :
    [[ -n "$DISCOVERONLY" ]] && return || true
    str=${1:-}
    regex=${2:-}
    expected=${3:-0}

    # status = 0 match, 1 = no match
    status=1

    # try the match. there's 3 possible outcomes:
    #
    # 1) it may succeed and be captured if user specified parentheses: status=0 and match="something"
    # 2) it may succeed and not be captured if parentheses not used: status=0 and match=""
    # 3) it may fail to match: status=1 and match=""

    match=""
    if [[ $str =~ $regex ]]; then
        status=0
    	match=${BASH_REMATCH[1]} # maybe some kind of capture is available
    fi

    if [[ "$status" -eq "$expected" ]]; then
        [[ -n "$DEBUG" ]] && echo -n . || true
        return
    fi

    if [ $status -eq 0 ] && [ -z "$match" ]; then
       if [[ $str =~ ($regex) ]]; then # the previous regex succeeded but no capture in $match, so try again with capturing parenthesess for debugging
    	  match=${BASH_REMATCH[1]}
       fi
    fi

    _assert_fail "match terminated with code $status and match '$match' instead of $expected" "$str" "$expected"
}

# do basic syntax check where exit code is expected to be 0
assert_raises "bash -n $program" 0

# connection test with listening server, exit code expected to be 0
assert_raises "$program $HOST system q" 0

# connection test with no listening server, exit code expected to be 1
assert_raises "$program $HOST_NOT_LISTENING system q" 1

# connection test with non-reachable server, exit code expected to be 1, but nodetool doesn't have a timeout, so don't run this
#assert_raises "$program $HOST_NOT_REACHABLE system q" 1

# check for malformed assignments, expected to match
out=`cat tests.txt`
assert_contains "$out" '\$[a-z0-9_]+=' 0

# check for malformed assignments, expected to not match
out=`cat cass_top`
assert_contains "$out" '\$[a-z0-9_]+=' 1

# check for unknown keyspace, expected to match
out=`$program $HOST zzzzzz f`
assert_contains "$out" "unknown keyspace" 0

# check for unknown command, expected to match
out=`$program $HOST system zzz`
assert_contains "$out" "unknown command" 0

# end of test suite
assert_end cass_top

