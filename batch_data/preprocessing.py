from pyspark.sql import SparkSession


if __name__ == "__main__":
    HDFS_PATH = "hdfs://localhost:9000"
    print("cao")
    spark = SparkSession.builder.appName("PreprocessData").getOrCreate()

    air_pollutants_df = spark.read.format("csv").option("header", "True").option("separator", ",").load(HDFS_PATH + "/batch/epa_hap_daily_summary.csv")

    # drop NULL columns
    air_pollutants_df = air_pollutants_df.drop("pollutant_standard")
    air_pollutants_df = air_pollutants_df.drop("aqi")
    air_pollutants_df = air_pollutants_df.drop("event_type")

    # drop not important columns
    air_pollutants_df = air_pollutants_df.drop("datum")
    air_pollutants_df = air_pollutants_df.drop("poc")
    air_pollutants_df = air_pollutants_df.drop("sample_duration")
    air_pollutants_df = air_pollutants_df.drop("first_max_hour")
    air_pollutants_df = air_pollutants_df.drop("method_code")
    air_pollutants_df = air_pollutants_df.drop("method_name")

    # TODO: finish preprocessing

    # save processed dataframe
    air_pollutants_df.write.format("csv").mode("overwrite").option("header", "True").save(HDFS_PATH + "/processed_data/epa_hap_daily_summary_processed.csv")
