#!/bin/bash

echo "Submitting Spark Job for Calculating Pi"

$SPARK_HOME/bin/spark-submit \
--class org.apache.spark.examples.SparkPi \
--master k8s://https://192.168.64.8:8443 \
--deploy-mode cluster \
--name spark-pi \
--conf spark.executor.instances=2 \
--conf spark.kubernetes.container.image=spark-hadoop:2.4.5 \
local:///opt/spark/examples/jars/spark-examples_2.11-2.4.5.jar