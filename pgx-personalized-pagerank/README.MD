# Personalized PageRank

Personalized PageRank computes node importance from a specific source node, favoring nearby nodes by simulating random walks that frequently return to the source, unlike global PageRank.

## Getting Started

You can get a quickstart graph server image (Podman) here: https://container-registry.oracle.com  
Go to Database > graph-quickstart
Graph Server version: oracle-graph-client-25.2.0

## Example Usage

```sh
podman run -d --name oraclegraph -p 1521:1521 -p 7007:7007 -e ORACLE_PWD=pass container-registry.oracle.com/database/graph-quickstart:latest
podman exec -it oraclegraph /opt/oracle/graph/scripts/quicksetup.sh -d
podman exec -it oraclegraph opg4j -b http://localhost:7007 #op4j
```
You should run [sample_data.sql](sample_data.sql) connected as graphuser on sqlcl or sqlplus.

Connect to http://localhost:7007/dash  for graph visualization

## Contents

- **PersonalizedPageRank.sh**: Script for running on op4j - [PersonalizedPageRank.sh](PersonalizedPageRank.sh)
- **PersonalizedPageRank.java**: Java version - [PersonalizedPageRank.java](PersonalizedPageRank.java)

---

## Sample Data

Query: `MATCH (customer)-[:purchased]->(product)`

| c_id        | p_id         | name                     |
|-------------|--------------|--------------------------|
| CUSTOMER(1) | PRODUCT(1)   | Notebook Dell XPS 13     |
| CUSTOMER(1) | PRODUCT(3)   | Cadeira Gamer ThunderX   |
| CUSTOMER(1) | PRODUCT(4)   | Fone de Ouvido JBL       |
| CUSTOMER(1) | PRODUCT(5)   | Monitor LG 27"           |
| CUSTOMER(2) | PRODUCT(2)   | Smartphone Samsung S23   |
| CUSTOMER(2) | PRODUCT(3)   | Cadeira Gamer ThunderX   |
| CUSTOMER(2) | PRODUCT(5)   | Monitor LG 27"           |
| CUSTOMER(3) | PRODUCT(1)   | Notebook Dell XPS 13     |
| CUSTOMER(3) | PRODUCT(2)   | Smartphone Samsung S23   |
| CUSTOMER(3) | PRODUCT(4)   | Fone de Ouvido JBL       |
| CUSTOMER(4) | PRODUCT(1)   | Notebook Dell XPS 13     |
| CUSTOMER(4) | PRODUCT(2)   | Smartphone Samsung S23   |
| CUSTOMER(4) | PRODUCT(5)   | Monitor LG 27"           |
| CUSTOMER(5) | PRODUCT(3)   | Cadeira Gamer ThunderX   |
| CUSTOMER(5) | PRODUCT(4)   | Fone de Ouvido JBL       |

---

## Personalized PageRank Result for Customer 5 (topK = 3)

| ID(p)       | name                   | pagerank               |
|-------------|------------------------|------------------------|
| PRODUCT(2)  | Smartphone Samsung S23 | 0.027750000000000004   |
| PRODUCT(1)  | Notebook Dell XPS 13   | 0.026687500000000003   |
| PRODUCT(5)  | Monitor LG 27"         | 0.026687500000000003   |

---

## Personalized PageRank Result for Customer 4 (topK = 3)

| ID(p)       | name                   | pagerank               |
|-------------|------------------------|------------------------|
| PRODUCT(4)  | Fone de Ouvido JBL     | 0.0288125              |
| PRODUCT(3)  | Cadeira Gamer ThunderX | 0.0288125              |

