# !/bin/bash

printf "\UPLOAD BATCH FILE TO HDFS\n"
sudo docker exec -it spark-master ./spark/bin/spark-submit ../spark/apps/upload_data.py

printf "\nPREPROCESS BATCH DATA\n"
sudo docker exec -it spark-master ./spark/bin/spark-submit ../spark/apps/preprocessing.py

printf "\nRUN BATCH PROCESSING\n"
sudo docker exec -it spark-master ./spark/bin/spark-submit  --jars ../spark/apps/postgresql-42.5.1.jar ../spark/apps/batch_processing.py

# printf "\RUN REAL-TIME PROCESSING\n"
# docker exec spark-master ./spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --jars ./spark/apps/postgresql-42.5.1.jar ./spark/apps/biggestHumidity.py  &
# docker exec spark-master ./spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --jars ./spark/apps/postgresql-42.5.1.jar ./spark/apps/averageWindSpeed.py & 
# docker exec spark-master ./spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --jars ./spark/apps/postgresql-42.5.1.jar ./spark/apps/sparkStreaming.py  