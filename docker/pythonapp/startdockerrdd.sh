docker stop spark-app-rdd
docker rm spark-app-rdd
docker run --name spark-app-rdd \
    -v $PWD/../../rddbasic:/app \
    --link spark-master:spark-master \
    -e ENABLE_INIT_DAEMON=false \
    -d spark-app