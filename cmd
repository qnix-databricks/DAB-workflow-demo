#!/bin/bash

set -x

echo "Build JAR ..."
sbt assembly || exit 1

echo "Build wheels ..."
cd python && python -m build --wheel

echo "Copying to Volume ..."
databricks --profile e2-demo fs cp --overwrite config/app_conf.json dbfs:/Volumes/users/quan_ta/config/
databricks --profile e2-demo fs cp --overwrite target/scala-2.13/workflow-demo-0.1.0.jar dbfs:/Volumes/users/quan_ta/jars/
databricks --profile e2-demo fs cp --overwrite python/dist/charter_utils-0.1.0-py3-none-any.whl dbfs:/Volumes/users/quan_ta/wheels/

echo "Deploy to databricks workspace ..."
databricks bundle deploy --profile e2-demo
