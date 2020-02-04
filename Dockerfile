FROM openjdk:8-jdk-alpine

ARG JAR_FILE=target/dedup-1.0-SNAPSHOT-jar-with-dependencies.jar

# cd /uc/dedup
WORKDIR /uc/dedup

# copy target/dedup-1.0-SNAPSHOT.jar /uc/dedup/app.jar
COPY ${JAR_FILE} app.jar

# What to run when the container starts
ENTRYPOINT [ "java", "-jar", "/uc/dedup/app.jar" ]

