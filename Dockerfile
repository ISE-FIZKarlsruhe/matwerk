FROM ghcr.io/epoz/shmarql:latest

# ---- App sources ----
COPY . /app

# ---- Shmarql docs build (mkdocs) ----
RUN mkdir -p /app/src/site/matwerk \
    && uv run mkdocs build -f /app/mkdocs.yml -d /app/src/site/matwerk