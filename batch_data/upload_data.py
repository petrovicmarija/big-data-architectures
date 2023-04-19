from pyspark.sql import SparkSession


if __name__ == "__main__":
    HDFS_PATH = "hdfs://localhost:9000"
    
    spark = SparkSession.builder.appName("UploadDataToHDFS").getOrCreate()

    historical_pollution_df = spark.read.format("csv").option("header", "True").option("separator", ",").load("epa_hap_daily_summary.csv")
    historical_pollution_df.write.format("csv").mode("overwrite").option("header", "True").save(HDFS_PATH + "/batch/epa_hap_daily_summary.csv")