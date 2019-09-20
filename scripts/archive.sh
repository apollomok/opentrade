#!/bin/bash
d=./archive/`date "+%Y%m%d-%H%M%S"`
mkdir -p $d
mv ./store $d
mv ./fixlog $d
mv ./fixstore $d
mv ./logs $d
tar czf $d.tar.gz $d && /bin/rm -rf $d
