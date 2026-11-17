FROM ghcr.io/epoz/shmarql:v1.0.1

# ---- App sources ----
COPY . /app

# ---- Java runtime (for HermiT / owlready2 build-time OWL reasoning) ----
USER root
RUN apt-get update && apt-get install -y --no-install-recommends default-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# ---- Shmarql docs build (mkdocs) ----
# shmarql v1.0.x no longer ships mkdocs-material: upstream moved its own
# rendering off mkdocs (mistletoe/MonsterUI). This site still uses the material
# theme and material.extensions.emoji in mkdocs.yml, so both are installed here
# explicitly rather than inherited from the base image.
RUN uv pip install mkdocs mkdocs-material mkdocs-macros-plugin "ontoink[reasoning]" pymdown-extensions \
    && mkdir -p /app/src/site/matwerk \
    && uv run mkdocs build -f /app/mkdocs.yml -d /app/src/site/matwerk