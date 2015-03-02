package com.sample.csv.spark

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object SparkCSVSample {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkCSVSample")
    val context = new SparkContext(conf)
    val (logDir, csvFilePath, targetUrls) = (args(0), args(1), args(2))

    val records = logFiles(logDir).map { logFile =>
      val logData = context.textFile(logFile)
      logData.map(line => LogParser.parse(line).getOrElse(Log())).filter(!_.time.isEmpty)
    }.reduce(_ ++ _)

    val counts = targetUrls.split(",").map { url =>
      val trimmedUrl = url.trim
      countsByUrl(records, trimmedUrl)
    }.reduce(_ ++ _)

    writeCSV(csvFilePath, counts)
  }

  private def logFiles(logDir: String): Seq[String] = new File(logDir).listFiles.map(_.getPath)

  private def countsByUrl(records: RDD[Log], url: String): RDD[String] = {
    val partitions = records.mapPartitions { partitionRecords =>
      val list = partitionRecords.toList
      val filteredRecords = list.filter(_.url == url)

      if (filteredRecords.isEmpty) {
        val time = list.map(_.time).head
        Iterator((time, 0))
      } else
        filteredRecords.map(record => (record.time, 1)).toIterator
    }

    partitions.repartition(1).reduceByKey(_ + _).sortBy(_._1).map(_._2).glom.map(record => s"$url,${record.mkString(",")}")
  }

  private def writeCSV(csvFilePath: String, countData: RDD[String]): Unit = {
    val tempFileDir = "/tmp/spark_temp"
    FileUtil.fullyDelete(new File(tempFileDir))
    FileUtil.fullyDelete(new File(csvFilePath))

    countData.saveAsTextFile(tempFileDir)
    merge(tempFileDir, csvFilePath)
  }

  private def merge(srcPath: String, dstPath: String): Unit = {
    val hadoopConfig = new Configuration
    val hdfs = FileSystem.get(hadoopConfig)
    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, hadoopConfig, null)
  }

}

