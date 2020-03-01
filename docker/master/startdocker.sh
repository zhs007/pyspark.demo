docker stop spark-master
docker rm spark-master
docker run --name spark-master \
    -h spark-master \
    -v $PWD/..:/app \
    -p 3722:8080 \
    -e ENABLE_INIT_DAEMON=false \
    -d spark-master