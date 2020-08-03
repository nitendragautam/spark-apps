#!/bin/bash

spark-submit \                                      
--master k8s://https://192.168.64.4:8443 \
--deploy-mode cluster \
--name spark-pi \
--class org.apache.spark.examples.SparkPi \
--conf spark.executor.instances=2 \
--conf spark.kubernetes.container.image=spark-hadoop:3.0.0 \
local:///opt/spark/examples/jars/spark-examples_2.12-3.0.0.jar
