FROM eclipse-temurin:25-jre

ENV LANGUAGE='en_US:en'

WORKDIR /deployments

# Four layers so library layers can be re-used when only app code changes.
COPY target/quarkus-app/lib/ /deployments/lib/
COPY target/quarkus-app/*.jar /deployments/
COPY target/quarkus-app/app/ /deployments/app/
COPY target/quarkus-app/quarkus/ /deployments/quarkus/

EXPOSE 8080

ENV JAVA_OPTS="-Dquarkus.http.host=0.0.0.0 \
-Djava.util.logging.manager=org.jboss.logmanager.LogManager \
-XX:+UseG1GC \
-XX:MaxRAMPercentage=75.0"

ENTRYPOINT [ "sh", "-c", "exec java $JAVA_OPTS -jar /deployments/quarkus-run.jar" ]
