FROM ghcr.io/epoz/shmarql:v0.69

# ---- App sources ----
COPY . /app

# ---- Shmarql docs build (mkdocs) ----
RUN uv pip install mkdocs-macros-plugin ontoink pymdown-extensions \
    && mkdir -p /app/src/site/matwerk \
    && uv run mkdocs build -f /app/mkdocs.yml -d /app/src/site/matwerk