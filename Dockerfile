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

# ---- Robot + Widoco ----
RUN set -eux; \
    wget -q https://github.com/ontodev/robot/releases/download/v1.9.8/robot.jar -O /usr/local/bin/robot.jar; \
    printf '#!/bin/sh\nexec java $ROBOT_JAVA_ARGS -jar /usr/local/bin/robot.jar "$@"\n' > /usr/local/bin/robot; \
    chmod +x /usr/local/bin/robot; \
    wget -q https://github.com/dgarijo/Widoco/releases/download/v1.4.25/widoco-1.4.25-jar-with-dependencies_JDK-11.jar -O /usr/local/bin/widoco.jar

WORKDIR /work

# ---- Python deps for your build scripts ----
COPY requirements.txt /work/requirements.txt
RUN pip install --no-cache-dir -r /work/requirements.txt

# ---- App sources ----
COPY . /work

# ---- Build KG ----
RUN set -eux; \
    python -m scripts.zenodo.export_zenodo --make-snapshots --out data/zenodo/zenodo.ttl; \
    python ./scripts/fetch_zenodo.py

RUN set -eux; \
    chmod +x /work/robot-download.sh /work/robot-merge.sh; \
    /work/robot-download.sh; \
    /work/robot-merge.sh; \
    test -s /work/data/all_NotReasoned.ttl

RUN set -eux; \
    chmod +x /work/robot-reason.sh; \
    /work/robot-reason.sh; \
    test -s /work/data/all.ttl

RUN set -eux; \
    printf '<https://purls.helmholtz-metadaten.de/msekg/all> {\n' > /work/data/all.trig; \
    cat /work/data/all.ttl >> /work/data/all.trig; \
    printf '\n}\n' >> /work/data/all.trig

RUN set -eux; \
    mkdir -p /data; \
    cp /work/data/all.ttl /data/ontology.ttl

# ---- Widoco ----
# We will publish it under /matwerk/ontology/
RUN set -eux; \
    rm -rf /work/public; \
    java -jar /usr/local/bin/widoco.jar \
      -ontFile /work/data/all.ttl \
      -outFolder /work/public \
      -uniteSections \
      -includeAnnotationProperties \
      -lang en-de \
      -getOntologyMetadata \
      -noPlaceHolderText \
      -rewriteAll \
      -webVowl; \
    test -s /work/public/index-en.html || test -s /work/public/index.html

# ---- Shmarql docs build (mkdocs) ----
# Build mkdocs in an isolated folder so we don't touch /app/src
RUN set -eux; \
    rm -rf /work/_mkdocs && mkdir -p /work/_mkdocs; \
    cp /work/mkdocs.yml /work/_mkdocs/mkdocs.yml; \
    cp -a /work/docs /work/_mkdocs/docs; \
    uv run python -m shmarql docs_build -f /work/_mkdocs/mkdocs.yml; \
    test -d /work/_mkdocs/site

RUN set -eux; \
    rm -rf /app/src/site/matwerk; \
    mkdir -p /app/src/site/matwerk; \
    cp -a /work/_mkdocs/site/. /app/src/site/matwerk/; \
    mkdir -p /app/src/site/matwerk/ontology; \
    cp -a /work/public/. /app/src/site/matwerk/ontology/


RUN set -eux; \
    mkdir -p /app/src/site/matwerk/static; \
    cp -a /app/src/static/. /app/src/site/matwerk/static/ || true

# ----runtime WORKDIR must be /app/src so shmarql finds "static/*" ----
WORKDIR /app/src
