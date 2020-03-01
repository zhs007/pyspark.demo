docker stop spark-app-local-rdd
docker rm spark-app-local-rdd
docker run --name spark-app-local-rdd \
    -v $PWD/../../rddbasic:/app \
    -e ENABLE_INIT_DAEMON=false \
    -d spark-app-local