#!bin/bash

docker exec -it resourcemanager bash -c "hdfs dfs -rm -r -f /batch*"
docker exec -it resourcemanager bash -c "hdfs dfs -rm -r -f /processed_data*"
docker exec -it resourcemanager bash -c "chmod +x /rawZone.sh && /rawZone.sh"