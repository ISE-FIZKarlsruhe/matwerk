# matwerk

Matwerk

For testing, you can run:

```shell
docker run --rm -it -p 8000:8000 -e DEBUG=1  \
    -e MOUNT=/matwerk/ -e DATA_LOAD_PATHS=/data/ -e PREFIXES_FILEPATH=/data/people.ttl \
    ghcr.io/ise-fizkarlsruhe/matwerk:latest
```

Now you can view the test site on: [https://localhost:8000/matwerk/](https://localhost:8000/matwerk/)
