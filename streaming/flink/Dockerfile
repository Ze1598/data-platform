# Flink image for the streaming/ module's Kafka -> Iceberg sink job.
#
# Job driver is a vendored Java program (sql-runner/, copied verbatim from
# apache/flink-kubernetes-operator's own examples/flink-sql-runner-example),
# not PyFlink -- switched from an earlier PyFlink-based design after
# PyFlink's `pemja` dependency turned out to have no Linux/arm64 wheels,
# forcing every image in this module onto a `--platform linux/amd64` cross-
# architecture build that then failed to load into this project's arm64
# kind cluster via the normal `kind load docker-image` path. See
# Learnings.md for the full investigation. The Java driver here is
# generic (reads any .sql file, executes its statements) and needs zero
# per-pipeline modification -- the actual Kafka->Iceberg logic lives
# entirely in sql-scripts/*.sql, plain SQL, not Java. This image builds
# natively for whatever architecture the host is on -- no --platform flag,
# no emulation, matching every other custom image in this repo.
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY sql-runner/pom.xml .
COPY sql-runner/src ./src
RUN mvn -B package -DskipTests

FROM flink:2.1.3-java17

RUN mkdir /opt/flink/usrlib
COPY --from=build /build/target/flink-sql-runner-*.jar /opt/flink/usrlib/sql-runner.jar
COPY sql-scripts /opt/flink/usrlib/sql-scripts

# Connector JARs -- downloaded pre-built artifacts, not hand-written code.
# Exact combination confirmed live against Maven Central (not guessed):
# iceberg-flink-runtime is published per-Flink-minor-version
# (iceberg-flink-runtime-2.1, only 1.11.0 published for it); the AWS/S3
# support Iceberg's own Flink+AWS docs (docs/docs/aws.md) document as the
# one required companion artifact is iceberg-aws-bundle (a self-contained
# shaded jar with the full AWS SDK v2 -- no separate Hadoop-shaded jar or
# raw AWS SDK bundle needed alongside it, unlike older Iceberg/Flink combos).
ADD https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.1/flink-sql-connector-kafka-5.0.0-2.1.jar /opt/flink/lib/
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-2.1/1.11.0/iceberg-flink-runtime-2.1-1.11.0.jar /opt/flink/lib/
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.11.0/iceberg-aws-bundle-1.11.0.jar /opt/flink/lib/
# Confirmed live (not from a tutorial): iceberg-flink-runtime's
# FlinkCatalogFactory.clusterHadoopConf() unconditionally instantiates
# org.apache.hadoop.conf.Configuration even for a REST catalog + S3FileIO
# that never touches HDFS -- ClassNotFoundException without it. The old
# flink-shaded-hadoop-2-uber (Hadoop 2.8.3, last published years ago)
# is the commonly-cited fix in older tutorials, but Hadoop's own modern
# hadoop-client-api/hadoop-client-runtime shaded jars (Hadoop 3.5.0,
# actively maintained) are the current equivalent and a better fit for a
# platform meant to be taken seriously, not just made to work once.
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/3.5.0/hadoop-client-api-3.5.0.jar /opt/flink/lib/
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.5.0/hadoop-client-runtime-3.5.0.jar /opt/flink/lib/
RUN chown flink:flink /opt/flink/lib/*.jar /opt/flink/usrlib/sql-runner.jar

USER flink
