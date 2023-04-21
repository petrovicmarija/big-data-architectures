from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, asc


# prosecna zagadjenost po drzavama
def get_pollution_by_country(df):

    result = df.groupBy("county_name").agg(avg("first_max_value").alias("average_pollution"))
    result = result.orderBy(result["county_name"].asc())

    save_to_postgres(result, "get_pollution_by_country")

    # result.write.format("jdbc").option("url", "jdbc:postgresql://db:5432/DATABASE").\
    #     option("driver", "org.postgresql.Driver").\
    #     option("dbtable", "get_pollution_by_country").\
    #     option("user", "user").\
    #     option("password", "password").\
    #     mode("overwrite").save()


# drzava sa najvecim brojem zagadjenih gradova
def get_country_with_most_cities_pollutant(df):

    result = df.groupBy("county_name").count()
    result = result.orderBy(result["county_name"].asc())
    result.show()
    
    save_to_postgres(result, "most_cities_pollutant")


def save_to_postgres(data, table_name):
        data.write.format("jdbc").option("url", "jdbc:postgresql://db:5432/DATABASE").\
        option("driver", "org.postgresql.Driver").\
        option("dbtable", table_name).\
        option("user", "user").\
        option("password", "password").\
        mode("overwrite").save()


if __name__ == "__main__":
    HDFS_PATH = "hdfs://namenode:9000/processed_data"

    spark = SparkSession.builder.appName("BatchProcessing").getOrCreate()

    air_pollutants_df = spark.read.format("csv").option("header", "True").option("separator", ",").load(HDFS_PATH + "/epa_hap_daily_summary_processed.csv")
    air_pollutants_df.show()

    air_pollutants_df.select(air_pollutants_df.city_name).show()

    get_pollution_by_country(air_pollutants_df)
    get_country_with_most_cities_pollutant(air_pollutants_df)