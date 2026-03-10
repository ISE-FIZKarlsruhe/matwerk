FROM ghcr.io/epoz/shmarql:v0.69

# ---- App sources ----
COPY . /app

# ---- Shmarql docs build (mkdocs) ----
RUN mkdir -p /app/src/site/matwerk \
    && uv run mkdocs build -f /app/mkdocs.yml -d /app/src/site/matwerk