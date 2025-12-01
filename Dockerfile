# ==========================
# Stage 1: Build KG (all.ttl)
# ==========================
# Use Python 3.11 base
FROM python:3.11-slim AS widoco

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        openjdk-17-jdk-headless \
        curl wget git unzip dos2unix \
    || { apt-get update; apt-get install -y --no-install-recommends default-jre-headless curl wget git unzip dos2unix; }; \
    rm -rf /var/lib/apt/lists/*


ENV ROBOT_JAVA_ARGS="-Xmx16G -Dfile.encoding=UTF-8"
WORKDIR /app

# Install Python deps (cached layer)
COPY requirements.txt /app/requirements.txt
RUN python -m pip install --no-cache-dir -r /app/requirements.txt

# Robot + Widoco
RUN wget -q https://github.com/ontodev/robot/releases/download/v1.9.8/robot.jar -O /usr/local/bin/robot.jar \
 && printf '#!/bin/sh\nexec java $ROBOT_JAVA_ARGS -jar /usr/local/bin/robot.jar "$@"\n' > /usr/local/bin/robot \
 && chmod +x /usr/local/bin/robot \
 && wget -q https://github.com/dgarijo/Widoco/releases/download/v1.4.25/widoco-1.4.25-jar-with-dependencies_JDK-11.jar

# App sources
COPY . /app

# Run your scripts (working dir aware)
#RUN python -m scripts.zenodo.export_zenodo --make-snapshots --out data/zenodo/zenodo.ttl && python ./scripts/fetch_zenodo.py


RUN chmod +x /app/robot-download.sh /app/robot-merge.sh \
 && ./robot-download.sh \
 && ./robot-merge.sh \
 && test -s data/all_NotReasoned.ttl

# endpoint fetch at build time
RUN chmod +x /app/scripts/fetch_endpoints.py \
 && python /app/scripts/fetch_endpoints.py

# reason the knowledge graph
RUN chmod +x /app/robot-reaon.sh\
 && ./robot-reaon.sh \
 && test -s data/all.ttl

 # Copy a predictable artifact
RUN mkdir -p /data && cp data/all.ttl /data/ontology.ttl

# Generate docs
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
FROM ghcr.io/epoz/shmarql:latest

COPY --from=widoco /app/data /data
COPY --from=widoco /app/docs /src/docs
COPY mkdocs.yml a.yml

RUN python -m shmarql docs_build -f a.yml

RUN mkdir /src/site/ontology
