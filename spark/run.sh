# !/bin/bash

printf "\RUN REAL-TIME PROCESSING\n"
docker exec spark-master ./spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --jars ./spark/apps/postgresql-42.5.1.jar ./spark/apps/most_present_pollutant.py &
docker exec spark-master ./spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --jars ./spark/apps/postgresql-42.5.1.jar ./spark/apps/average_concentration.py & 
docker exec spark-master ./spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --jars ./spark/apps/postgresql-42.5.1.jar ./spark/apps/calculate_aqi.py  