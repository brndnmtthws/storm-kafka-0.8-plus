storm-kafka-0.8-plus
====================

Port of storm-kafka to support kafka >= 0.8.

## Grab it from [clojars.org](https://clojars.org/)

```xml
  <repositories>
    <repository>
      <id>clojars.org</id>
      <url>http://clojars.org/repo</url>
    </repository>
  </repositories>
  ...
  <dependencies>
    <dependency>
      <groupId>org.clojars.brenden</groupId>
      <artifactId>storm-kafka-0.8-plus</artifactId>
			<version>0.1.8-SNAPSHOT</version>
    </dependency>
  </dependencies>
```

## Installing Kafka 0.8 into local maven repository

If you want to build storm-kafka-0.8-plus yourself, you can try these steps:

```
wget -q -c https://github.com/airbnb/kafka/archive/production.tar.gz
tar xf production.tar.gz
cd kafka-production
./sbt "++2.10.3 release"
./sbt "++2.10.3 make-pom"
mvn install:install-file -DgroupId=org.apache.kafka -DartifactId=kafka_2.10 -Dversion="0.8.1" -Dpackaging=jar -Dfile=core/target/scala-2.10/kafka_2.10-0.8.1.jar -DpomFile=core/target/scala-2.10/kafka_2.10-0.8.1.pom
cd ..
mvn package
```
