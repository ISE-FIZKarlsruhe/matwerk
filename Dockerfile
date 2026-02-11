FROM ghcr.io/epoz/shmarql:v0.67

# ---- System deps: Java + tooling ----
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
      ca-certificates \
      openjdk-21-jre-headless \
      curl wget git unzip dos2unix \
    ; \
    rm -rf /var/lib/apt/lists/*

ENV ROBOT_JAVA_ARGS="-Xmx16G -Dfile.encoding=UTF-8"
WORKDIR /app

# ---- Python deps for your build scripts ----
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# ---- Robot + Widoco ----
RUN wget -q https://github.com/ontodev/robot/releases/download/v1.9.8/robot.jar -O /usr/local/bin/robot.jar \
 && printf '#!/bin/sh\nexec java $ROBOT_JAVA_ARGS -jar /usr/local/bin/robot.jar "$@"\n' > /usr/local/bin/robot \
 && chmod +x /usr/local/bin/robot \
 && wget -q https://github.com/dgarijo/Widoco/releases/download/v1.4.25/widoco-1.4.25-jar-with-dependencies_JDK-11.jar -O /usr/local/bin/widoco.jar

# ---- App sources ----
COPY . /app

# ---- Build KG ----
RUN python -m scripts.zenodo.export_zenodo --make-snapshots --out data/zenodo/zenodo.ttl \
 && python ./scripts/fetch_zenodo.py

RUN chmod +x /app/robot-download.sh /app/robot-merge.sh \
 && ./robot-download.sh \
 && ./robot-merge.sh \
 && test -s data/all_NotReasoned.ttl

# RUN set -eux; \
#     chmod +x /app/scripts/fetch_endpoints.py; \
#     mkdir -p /app/data/sparql_endpoints/named_graphs; \
#     python /app/scripts/fetch_endpoints.py \
#       --all-ttl /app/data/all_NotReasoned.ttl \
#       --mwo-owl /app/ontology/mwo-full.owl \
#       --state-json /app/data/sparql_endpoints/sparql_sources.json \
#       --summary-json /app/data/sparql_endpoints/sparql_sources_list.json \
#       --stats-ttl /app/data/sparql_endpoints/dataset_stats.ttl \
#       --named-graphs-dir /app/data/sparql_endpoints/named_graphs

RUN set -eux; \
    chmod +x /app/robot-reason.sh; \
    ./robot-reason.sh; \
    test -s data/all.ttl

RUN printf '<https://purls.helmholtz-metadaten.de/msekg/all> {\n' > data/all.trig \
 && cat data/all.ttl >> data/all.trig \
 && printf '\n}\n' >> data/all.trig
RUN mkdir -p /data && cp data/all.ttl /data/ontology.ttl

# ---- Widoco ----
RUN java -jar /usr/local/bin/widoco.jar \
    -ontFile /app/data/all.ttl \
    -outFolder public \
    -uniteSections \
    -includeAnnotationProperties \
    -lang en-de \
    -getOntologyMetadata \
    -noPlaceHolderText \
    -rewriteAll \
    -webVowl

# ---- Shmarql docs build (mkdocs) ----
COPY mkdocs.yml /app/a.yml
COPY docs /src/docs
RUN uv run python -m shmarql docs_build -f /app/a.yml

# ---- Integrate Widoco output into the final site ----
RUN mkdir -p /src/site/ontology \
 && cp -r /app/public/doc/* /src/site/ontology/ \
 && cp /src/site/ontology/index-en.html /src/site/ontology/index.html


