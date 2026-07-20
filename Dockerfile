FROM ghcr.io/epoz/shmarql:v0.69

# ---- App sources ----
COPY . /app

# ---- Java runtime (for HermiT / owlready2 build-time OWL reasoning) ----
USER root
RUN apt-get update && apt-get install -y --no-install-recommends default-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# ---- Shmarql docs build (mkdocs) ----
RUN uv pip install mkdocs-macros-plugin "ontoink[reasoning]" pymdown-extensions \
    && mkdir -p /app/src/site/matwerk \
    && uv run mkdocs build -f /app/mkdocs.yml -d /app/src/site/matwerk