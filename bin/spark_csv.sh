#!/bin/sh

CSV_FILE_PATH=$1
TARGET_URLS=$2
EXEC_DIR=`dirname ${0}`
BASE_DIR="${EXEC_DIR}/.."

spark-submit --class com.sample.csv.spark.SparkCSVSample --master "local[2]" $BASE_DIR/target/scala-2.11/spark_csv_sample_2.11-1.0.0.jar $BASE_DIR/logs $CSV_FILE_PATH $TARGET_URLS
