docker stop spark-master
docker rm spark-master
docker run --name spark-master \
    -h spark-master \
    -v $PWD/..:/app \
    -e ENABLE_INIT_DAEMON=false \
    -d spark-master