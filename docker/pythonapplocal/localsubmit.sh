#!/bin/bash

export SPARK_HOME=/spark

PYSPARK_PYTHON=python3 /spark/bin/spark-submit \
    ${SPARK_SUBMIT_ARGS} \
    ${SPARK_APPLICATION_PYTHON_LOCATION} ${SPARK_APPLICATION_ARGS}