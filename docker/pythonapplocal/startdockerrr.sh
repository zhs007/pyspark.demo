docker stop spark-app-local-rr
docker rm spark-app-local-rr
docker run --name spark-app-local-rr \
    -v $PWD/../../retentionrate:/app \
    -e ENABLE_INIT_DAEMON=false \
    -d spark-app-local