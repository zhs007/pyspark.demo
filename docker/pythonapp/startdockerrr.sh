docker stop spark-app-rr
docker rm spark-app-rr
docker run --name spark-app-rr \
    -h spark-app \
    -v $PWD/../retentionrate:/app \
    --link spark-master:spark-master \
    -e ENABLE_INIT_DAEMON=false \
    -d spark-app