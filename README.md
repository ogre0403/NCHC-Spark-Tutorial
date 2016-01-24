Spark-sample
===============

1. Download VM lab environment using vagrant
```sh
    vagrant up {spark-master|spark-slave}
```

2.  Use sbt to build and run scala example
```sh
    sbt clean package
```
```sh
    $SPARK_HOME/bin/spark-submit \
      --class org.nchc.spark.scala.HelloWorld \
      ./target/scala-2.10/spark-sample_2.10-0.0.1.jar \
      ./README.md
```

3.  Use Maven to build and run Java example
```sh

	mvn clean; mvn compile; mvn package
```
```sh
    $SPARK_HOME/bin/spark-submit \
    --class org.nchc.spark.java.first.HelloWorld \
      ./target/spark-sample-0.0.1.jar \
      ./README.md
```
```sh
    $SPARK_HOME/bin/spark-submit \
      --class org.nchc.spark.java.first.HelloWorldLambda \
      ./target/spark-sample-0.0.1.jar \
      ./README.md
```

4. Run Python example
```sh
    $SPARK_HOME/bin/spark-submit \
    ./python/HwlloWorld.py \
    ./README.md
```