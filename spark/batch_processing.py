from pyspark.sql import SparkSession


if __name__ == "__main__":
    HDFS_PATH = "hdfs://localhost:9000/processed_data"

    spark = SparkSession.builder.appName("BatchProcessing").getOrCreate()

    air_pollutants_df = spark.read.format("csv").option("header", "True").option("separator", ",").load(HDFS_PATH + "/epa_hap_daily_summary_processed.csv")
    air_pollutants_df.show()

