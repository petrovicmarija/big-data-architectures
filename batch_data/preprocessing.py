from pyspark.sql import SparkSession


if __name__ == "__main__":
    HDFS_PATH = "hdfs://localhost:9000"

    spark = SparkSession.builder.appName("PreprocessData").getOrCreate()

    from_hdfs_df = spark.read.format("csv").option("header", "True").option("separator", ",").load(HDFS_PATH + "/batch/epa_hap_daily_summary.csv")

    #TODO: finish preprocessing