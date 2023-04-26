# !/bin/bash

printf "\UPLOAD BATCH FILE TO HDFS\n"
sudo docker exec -it spark-master ./spark/bin/spark-submit ../spark/apps/upload_data.py

printf "\nPREPROCESS BATCH DATA\n"
sudo docker exec -it spark-master ./spark/bin/spark-submit ../spark/apps/preprocessing.py

printf "\nRUN BATCH PROCESSING\n"
sudo docker exec -it spark-master ./spark/bin/spark-submit  --jars ../spark/apps/postgresql-42.5.1.jar ../spark/apps/batch_processing.py
