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

# ---- App sources ----
COPY . /app

# ---- Widoco ----
RUN wget -q https://github.com/dgarijo/Widoco/releases/download/v1.4.25/widoco-1.4.25-jar-with-dependencies_JDK-11.jar -O /usr/local/bin/widoco.jar

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
COPY mkdocs.yml /app/src/a.yml
COPY docs /app/src/docs
RUN uv run python -m shmarql docs_build -f /app/src/a.yml
RUN mv /app/src/site /app/src/__ && mkdir /app/src/site/ && mv /app/src/__ /app/src/site/matwerk

