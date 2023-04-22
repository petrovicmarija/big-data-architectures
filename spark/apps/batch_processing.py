from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, split, year, sum, desc, max, countDistinct, col


# 1) U kojim kolicinama su zastupljeni zagadjivaci u kojim gradovima?
def pollutants_in_cities(df):
    pollutants_df = df.groupBy("city_name", "parameter_name").agg(sum("first_max_value").alias("total_concentration"))

    pollutants_df.write.format("jdbc").option("url", "jdbc:postgresql://db:5432/DATABASE").\
    option("driver", "org.postgresql.Driver").\
    option("dbtable", "pollutants_in_cities").\
    option("user", "user").\
    option("password", "password").\
    mode("overwrite").save()


# 2) Prosecna godisnja koncentracija cestica za svaki grad
def average_yearly_concentration(df):
    date_df = df.withColumn("date_local", df["date_local"].cast("date"))
    avg_df = date_df.groupBy("city_name", "parameter_name", year("date_local")).agg(avg("first_max_value").alias("average_pollution"))

    avg_df.write.format("jdbc").option("url", "jdbc:postgresql://db:5432/DATABASE").\
    option("driver", "org.postgresql.Driver").\
    option("dbtable", "average_yearly_concentration").\
    option("user", "user").\
    option("password", "password").\
    mode("overwrite").save()


# 3) Pet gradova sa najzagadjenijim vazduhom
def five_cities_with_the_most_pollutant_air(df):
    casted_df = df.withColumn("first_max_value", col("first_max_value").cast("float"))

    pollutants_df = casted_df.groupBy("city_name").sum("first_max_value").withColumnRenamed("sum(first_max_value)", "total_concentration")
    top5_df = pollutants_df.sort(desc("total_concentration")).limit(5)

    top5_df.write.format("jdbc").option("url", "jdbc:postgresql://db:5432/DATABASE").\
    option("driver", "org.postgresql.Driver").\
    option("dbtable", "five_cities_with_the_most_pollutant_air").\
    option("user", "user").\
    option("password", "password").\
    mode("overwrite").save()


# 4) Geografska sirina i duzina mesta sa najstetnijim vazduhom
def find_long_lat_of_the_most_pollutant_place(df):
    casted_df = df.withColumn("first_max_value", col("first_max_value").cast("float"))

    long_lat_grouped_df = casted_df.groupBy("latitude", "longitude").sum("first_max_value").withColumnRenamed("sum(first_max_value)", "total_concentration")
    first_place_df = long_lat_grouped_df.sort(desc("total_concentration")).limit(1)

    first_place_df.write.format("jdbc").option("url", "jdbc:postgresql://db:5432/DATABASE").\
    option("driver", "org.postgresql.Driver").\
    option("dbtable", "lat_long_of_the_most_pollutant_place").\
    option("user", "user").\
    option("password", "password").\
    mode("overwrite").save()


# 5) Najveci broj izvrsenih merenja u jednom danu
def find_max_measurements_in_a_day(df):
    count_measurements_df = df.groupBy("date_local").agg(countDistinct("parameter_name")).withColumnRenamed("count(parameter_name)", "measurements_num")
    max_measurements_df = count_measurements_df.groupBy().agg(max("measurements_num")).withColumnRenamed("max(measurements_num", "max_measurements")
    
    max_measurements_df.write.format("jdbc").option("url", "jdbc:postgresql://db:5432/DATABASE").\
    option("driver", "org.postgresql.Driver").\
    option("dbtable", "max_measurements").\
    option("user", "user").\
    option("password", "password").\
    mode("overwrite").save()


# 6) Koje godine je izmereno najvece zagadjenje
def get_the_most_pollutant_year(df):
    df_with_year = df.withColumn("year", split(df["date_local"], "-").getItem(0)) 
    df_with_year.show()

    result = df_with_year.groupBy("year").agg(avg("first_max_value").alias("average_pollution"))
    result = result.sort(result["average_pollution"].desc()).limit(20)
    result.show()

    result.write.format("jdbc").option("url", "jdbc:postgresql://db:5432/DATABASE").\
        option("driver", "org.postgresql.Driver").\
        option("dbtable", "years_with_the_most_pollutions").\
        option("user", "user").\
        option("password", "password").\
        mode("overwrite").save()



# 7) Drzava sa najvecim brojem zagadjenih gradova
def get_country_with_most_cities_pollutant(df):
    result = df.groupBy("county_name").count()
    result = result.orderBy(result["county_name"].asc())

    result.write.format("jdbc").option("url", "jdbc:postgresql://db:5432/DATABASE").\
    option("driver", "org.postgresql.Driver").\
    option("dbtable", "country_with_most_cities_pollutant").\
    option("user", "user").\
    option("password", "password").\
    mode("overwrite").save()


# 8) Prosecna zagadjenost vazduha po drzavama
def get_pollution_by_country(df):
    result = df.groupBy("county_name").agg(avg("first_max_value").alias("average_pollution"))
    result = result.orderBy(result["county_name"].asc())

    result.write.format("jdbc").option("url", "jdbc:postgresql://db:5432/DATABASE").\
    option("driver", "org.postgresql.Driver").\
    option("dbtable", "pollution_by_country").\
    option("user", "user").\
    option("password", "password").\
    mode("overwrite").save()


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
    get_the_most_pollutant_year(air_pollutants_df)
    pollutants_in_cities(air_pollutants_df)
    average_yearly_concentration(air_pollutants_df)
    five_cities_with_the_most_pollutant_air(air_pollutants_df)
    find_long_lat_of_the_most_pollutant_place(air_pollutants_df)
    find_max_measurements_in_a_day(air_pollutants_df)
