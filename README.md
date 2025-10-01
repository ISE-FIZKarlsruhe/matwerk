[![shacl validation](https://github.com/ISE-FIZKarlsruhe/matwerk/actions/workflows/kg-validation.yml/badge.svg)](https://github.com/ISE-FIZKarlsruhe/matwerk/actions/workflows/kg-validation.yml)

# MatWerk Knowledge Graph

This repository contains the ontology and data that form the **MatWerk Knowledge Graph** for [NFDI-MatWerk](https://nfdi-matwerk.de/), a consortium of the National Research Data Infrastructure (NFDI) focused on materials science and engineering.

---

## 🔍 Live Deployment

You can explore the deployed version of the knowledge graph here:

👉 [https://nfdi.fiz-karlsruhe.de/matwerk/](https://nfdi.fiz-karlsruhe.de/matwerk/)

---

## 🚀 Quick Start (Test Environment)

To run a local test instance using Docker:

```shell
docker build -t ghcr.io/ise-fizkarlsruhe/matwerk:latest .

docker run --rm -it -p 8000:8000 -e DEBUG=1 -e MOUNT=/matwerk/ -e DATA_LOAD_PATHS=/data/all.ttl -e PREFIXES_FILEPATH=/data/all.ttl -v "$(pwd)/data:/data" ghcr.io/ise-fizkarlsruhe/matwerk:latest
```

After starting, you can access it at:

🔗 [http://127.0.0.1:8000/matwerk/](http://127.0.0.1:8000/matwerk/)

---

## 📁 Directory Structure

```bash
📦matwerk/
 ┣ 📂data/             # Combined TTL file for local testing
 ┣ 📂shapes/           # SHACL shapes
 ┣ 📄Dockerfile        # Docker configuration (if building locally)
 ┗ 📄README.md         # This file
```

---

## ⚙️ Technologies Used

- **ROBOT** for ontology templating and management
- **SHACL** for validation of instance data
- **BFO**, **IAO**, and NFDI Ontologies for modeling
- **SHMARQL** for serving and querying the data (via Docker)

---

## 📫 Contact

For questions or support, please create issues or contact Ebrahim Norouzi `ebrahim.norouzi@fiz-karlsruhe.de`.

## TODO
- run reasoner first for ontology, then all, how we manage reasoning
- We need documentation for testing the reasonner, consider examples and test
- SPARQL endpoint downloader (named graph)
- Zenodo downloader (done!)