FROM openjdk:jre AS build-stage

# Install prerequisites
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    python3 \
    python3-pip \
    git \
    unzip \
    dos2unix \
    && pip3 install pyshacl

# Get ROBOT jar
RUN wget https://github.com/ontodev/robot/releases/download/v1.9.5/robot.jar -O /usr/local/bin/robot.jar
ENV ROBOT_JAVA_ARGS="-Xmx8G -Dfile.encoding=UTF-8"

# Make robot command available
RUN echo '#!/bin/bash\njava $ROBOT_JAVA_ARGS -jar /usr/local/bin/robot.jar "$@"' > /usr/local/bin/robot && chmod +x /usr/local/bin/robot

# Prepare workspace
WORKDIR /app
COPY . .

# Convert Windows line endings to Unix
RUN dos2unix ./1st-kg.sh ./2nd-merge-all.sh

# Run the two bash scripts to generate all.ttl
RUN chmod +x ./1st-kg.sh && ./1st-kg.sh
RUN chmod +x ./2nd-merge-all.sh && ./2nd-merge-all.sh

# Widoco to generate documentation
RUN wget https://github.com/dgarijo/Widoco/releases/download/v1.4.25/widoco-1.4.25-jar-with-dependencies_JDK-11.jar
RUN java -jar widoco-1.4.25-jar-with-dependencies_JDK-11.jar \
    -ontFile data/all.ttl \
    -outFolder public \
    -uniteSections \
    -includeAnnotationProperties \
    -lang en-de \
    -getOntologyMetadata \
    -noPlaceHolderText \
    -rewriteAll \
    -webVowl

# Now move to final stage
FROM ghcr.io/epoz/shmarql:v0.56
COPY ./data/all.ttl /data/ontology.ttl
COPY docs /src/docs
COPY mkdocs.yml a.yml
RUN python -m shmarql docs_build -f a.yml

RUN mkdir /src/site/ontology
