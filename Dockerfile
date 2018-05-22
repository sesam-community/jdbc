FROM maven:3.5-jdk-8 AS build
COPY . /project
WORKDIR /project
RUN mvn -f pom.xml package


FROM java:8-jre-alpine
COPY --from=build /project/target/jdbc-datasource-template-1.0-SNAPSHOT.jar /srv/jdbc-datasource-template-1.0-SNAPSHOT.jar

EXPOSE 4567
ENTRYPOINT ["java", "-jar", "/srv/jdbc-datasource-template-1.0-SNAPSHOT.jar"]


