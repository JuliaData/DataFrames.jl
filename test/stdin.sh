#!/bin/bash

TESTDIR=$(dirname $0)
cat $TESTDIR/data/utf8/utf8.csv | julia $TESTDIR/stdin.jl
