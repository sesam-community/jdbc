FROM java:8-jre-alpine

ADD target/jdbc-datasource-template-1.0-SNAPSHOT.jar /srv/

EXPOSE 4567
ENTRYPOINT ["java", "-jar", "/srv/jdbc-datasource-template-1.0-SNAPSHOT.jar"]


