#!/bin/sh
rm -rf ../java/cn/edu/thssdb/rpc
rm -rf gen-java
thrift -r --gen java rpc.thrift
mv gen-java/cn/edu/thssdb/rpc/ ../java/cn/edu/thssdb/
