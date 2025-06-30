# ==========================
# Stage 1: Build KG (all.ttl)
# ==========================
FROM openjdk:17-slim AS widoco

# Install prerequisites
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    python3 \
    python3-pip \
    git \
    unzip \
    dos2unix \
    && pip3 install pyshacl \
    && apt-get clean

ENV ROBOT_JAVA_ARGS="-Xmx8G -Dfile.encoding=UTF-8"

# Download ROBOT
RUN wget https://github.com/ontodev/robot/releases/download/v1.9.8/robot.jar -O /usr/local/bin/robot.jar
RUN echo '#!/bin/bash\njava $ROBOT_JAVA_ARGS -jar /usr/local/bin/robot.jar "$@"' > /usr/local/bin/robot && chmod +x /usr/local/bin/robot

# Set workdir and copy all files
WORKDIR /app
COPY . .

# Prepare scripts and run them
RUN chmod +x ./1st-kg.sh && ./1st-kg.sh
RUN chmod +x ./2nd-merge-all.sh && ./2nd-merge-all.sh
RUN cp data/all.ttl /data/ontology.ttl

# Download Widoco
RUN wget https://github.com/dgarijo/Widoco/releases/download/v1.4.25/widoco-1.4.25-jar-with-dependencies_JDK-11.jar

# Run Widoco to generate docs in /app/docs
RUN java -jar widoco-1.4.25-jar-with-dependencies_JDK-11.jar \
    -ontFile data/all.ttl \
    -outFolder docs \
    -uniteSections \
    -includeAnnotationProperties \
    -lang en-de \
    -getOntologyMetadata \
    -noPlaceHolderText \
    -rewriteAll \
    -webVowl

# ==========================
# Stage 2: Run Shmarql
# ==========================
FROM ghcr.io/epoz/shmarql:v0.56

COPY --from=widoco /app/data /data
COPY --from=widoco /app/docs /src/docs
COPY --from=widoco /app/mkdocs.yml /a.yml

RUN python -m shmarql docs_build -f a.yml

RUN mkdir /src/site/ontology
