#!/bin/bash

# usage: ./tests.sh [host=localhost]

HOST=${1:-localhost}
HOST_KNOWN_BAD="1.2.3.4"

program="./cass_top"

. assert.sh

assert_contains() {
    # assert_contains <str> <regex> [expected]
    (( tests_ran++ )) || :
    [[ -n "$DISCOVERONLY" ]] && return || true
    str=${1:-}
    regex=${2:-}
    expected=${3:-0}

    status=1

    if [[ "$str" == *$regex* ]]; then
        status=0
    fi

    if [[ "$status" -eq "$expected" ]]; then
        [[ -n "$DEBUG" ]] && echo -n . || true
        return
    fi
    _assert_fail "match terminated with code $status instead of $expected" "$1" "$3"
}

# do basic syntax check where exit code is expected to be 0
assert_raises "bash -n $program"

# exit code expected to be 0
assert_raises "$program $HOST system q"

# exit code expected to be 1, but nodetool doesn't have a timeout, so don't run this
#assert_raises "$program $HOST_KNOWN_BAD" 1

# expected to match "unknown keyspace" on stdout
out=`$program $HOST zzzzzz f`
assert_contains "$out" "unknown keyspace"

out=`$program $HOST system zzz`
# expected to match "unknown command" on stdout
assert_contains "$out" "unknown command"

# end of test suite
assert_end cass_top

