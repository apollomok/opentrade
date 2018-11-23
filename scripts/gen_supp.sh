#!/bin/sh
valgrind --gen-suppressions=all --log-fd=9 ./opentrade 9>memcheck.log
grep -v == memcheck.log >> .valgrind-python.supp
/bin/rm memcheck.log
