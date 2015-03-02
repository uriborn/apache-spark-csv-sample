name := "spark_csv_sample"

version := "1.0.0"

scalaVersion := "2.11.2"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-hdfs" % "2.6.0",
  "org.apache.spark" %% "spark-core"  % "1.2.1"
)
