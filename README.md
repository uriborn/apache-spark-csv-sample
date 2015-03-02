# apache-spark-csv-sample
Apache Spark standalone script with output CSV file.
Analyze target logs, and counts by url each days.

## Usage

```
$ cd ${THIS_PROJECT_PATH}
$ activator package
$ ./bin/spark_csv.sh [output_csv_file_path] [target_urls(Comma separated)]
```

e.g)

```
$ ./bin/spark_csv.sh /tmp/spark_csv_sample.csv /hoge,/piyo
```

