#!/bin/sh

TESTDIR=$(dirname $0)
cat $TESTDIR/data/utf8/utf8.csv | $JULIA_HOME/julia $TESTDIR/stdin.jl
