# Notes

## Spark

### Cluster Managers

- https://spark.apache.org/docs/latest/spark-standalone.html
- https://spark.apache.org/docs/latest/running-on-yarn.html
- https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html
- https://spark.apache.org/docs/preview/running-on-mesos.html
- https://mesos.apache.org/

### Packaging Scala App to executable `.jar`

In IntelliJ, add JAR artifact configuration and then build the artifacts.
This should create a `out` folder in the project directory.

`out/artifacts/spark_essentials_jar/spark-essentials.jar`
will contain all the apps and classes we've built throughout the course
in JVM bytecode. Copy this file to `spark-cluster/apps`.

- https://stackoverflow.com/questions/12079230/what-exactly-does-a-jar-file-contain
- https://wikiless.org/wiki/JAR_(file_format)?lang=en

Also copy `movies.json` to `spark-cluster/data`.
This will let us access the files in the spark containers:

```dockerfile
# Easy way to exchange files within containers (no ssh/scp).
volumes:
   - ./apps:/opt/spark-apps
   - ./data:/opt/spark-data
```

We can then run `DeployApp.scala`
in the docker cluster with `/spark/bin/spark-submit`:

```bash
$ docker exec -it spark-cluster-spark-master-1 bash
$ cd /spark
$ ./bin/spark-submit \
  --class section6.DeployApp \
  --deploy-mode client \
  --master spark://8f36cf4da4b4:7077 \
  --verbose \
  --supervise \
   /opt/spark-apps/spark-essentials.jar \
   /opt/spark-data/movies.json \
   /opt/spark-data/good_comedies.json

# 8f36cf4da4b4 = Master node's hostname (cat /etc/hsotname)
```
