# for endpoints harvester
python .\scripts\fetch_endpoints.py

# for zenodo harvester
python -m scripts.zenodo.export_zenodo --make-snapshots --out data/zenodo/zenodo.ttl

# zenodo harvester: By DOI or record URL
python -m scripts.zenodo.export_zenodo --doi 10.5281/zenodo.13797439 --make-snapshots --out data/zenodo/zenodo-bydoi.ttl
python -m scripts.zenodo.export_zenodo --record-url https://zenodo.org/record/13797439 --make-snapshots --out data/zenodo/zenodo-bydoi.ttl
