docker stop spark-worker
docker rm spark-worker
docker run --name spark-worker \
    --link spark-master:spark-master \
    -e ENABLE_INIT_DAEMON=false \
    -d spark-worker