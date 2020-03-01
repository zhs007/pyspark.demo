docker stop spark-app-mysql
docker rm spark-app-mysql
docker run --name spark-app-mysql \
    -v $PWD/../../mysqlbasic:/app \
    --link spark-master:spark-master \
    -e ENABLE_INIT_DAEMON=false \
    -d spark-app