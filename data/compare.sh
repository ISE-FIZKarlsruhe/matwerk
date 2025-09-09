#!/bin/bash
#sudo apt-get update
#sudo apt-get install graphviz graphviz-dev
#pip install pygraphviz

python3 kg_compare.py \
  --kg1 all.ttl --name1 BFO_MSE \
  --kg2 MSE_KG_old/mse_v1.ttl --name2 SCHEMA_MSE \
  --label "Ebrahim Norouzi" \
  --type1 https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000004 \
  --type2 https://nfdi.fiz-karlsruhe.de/ontology/Person \
  --hops 1 --limit 8000 --direction out \
  --only-individuals \
  --out out

