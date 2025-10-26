FROM eclipse-temurin:21-jdk
WORKDIR /app
COPY build/libs/*.jar app.jar
EXPOSE 8077
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar /app/app.jar"]

