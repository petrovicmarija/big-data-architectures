from pyspark.sql import SparkSession
from pyspark.sql.functions import *

kafka_topic = "air-pollutants-topic"
kafka_bootstrap_servers = "kafka2:19093"


def write_result(result, epoch, table_name):

    result.write.format("jdbc").\
        option("url", "jdbc:postgresql://db:5432/DATABASE").\
        option("driver", "org.postgresql.Driver").\
        option("dbtable", table_name).\
        option("user", "user").\
        option("password", "password").\
        mode("overwrite").save()
    

if __name__ == "__main__":
    print("Welcome to Spark Structured Streaming!")

    spark = SparkSession.builder.appName("Spark streaming").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafka_bootstrap_servers)\
                .option("subscribe", kafka_topic).option("startingOffsets", "latest").load()
    
    df.printSchema()

    df = df.selectExpr("CAST(value AS STRING)")

    schema_string = "pollutant_location_id STRING, pollutant_location STRING, pollutant_parameter STRING, "\
            + "pollutant_value DOUBLE, pollutant_date TIMESTAMP, pollutant_unit STRING, pollutant_coordinates_latitude DOUBLE, "\
            + "pollutant_coordinate_longitude DOUBLE, pollutant_country STRING, pollutant_city STRING, "\
            + "pollutant_is_mobile BOOLEAN, pollutant_is_analysis BOOLEAN, pollutant_entity STRING, pollutant_sensor_type STRING"
    
    df = df.select(from_csv(col("value"), schema_string).alias("pollutant"))

    df = df.select("pollutant.*" )
    df.printSchema()

    most_present_pollutant_df = df.groupBy("pollutant_date").agg({"pollutant_value": "max", "pollutant_parameter": "first", "pollutant_unit": "first"}) \
                                .select("pollutant_date", col("max(pollutant_value)").alias("concentration"))
    
    query = most_present_pollutant_df.writeStream.outputMode("update") \
        .foreachBatch(lambda df, epoch_id: write_result(df, epoch_id, "most_present_pollutant")) \
        .start()

    query.awaitTermination()

    print("Stream data processing complete")