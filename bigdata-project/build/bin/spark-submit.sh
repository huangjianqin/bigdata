#!/usr/bin/env bash

#################
# $1: spark application(driver) main class
# $2: $1所属的jar包
#################

base_dir=$(dirname $0)/..

# --jars的话, 需要自己拼出jars路径string, 逗号分隔
# 仅仅适用于Client mode, Cluster mode只能把jars复制到lib下
cmd=`spark-submit --conf spark.driver.extraClassPath=${base_dir}/lib/*.jar \
--conf spark.executor.extraClassPath=${base_dir}/lib/*.jar \
--class $1 $2 2>&1`

# run
`$cmd`