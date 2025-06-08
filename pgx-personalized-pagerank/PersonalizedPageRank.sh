# Start container
podman run -d --name oraclegraph -p 1521:1521 -p 7007:7007 -e ORACLE_PWD=pass container-registry.oracle.com/database/graph-quickstart:latest
# Connected as sys or system on sqlplus, please grant PGX_SESSION_ADD_PUBLISHED_GRAPH to graphuser;

# disable TLS
podman exec -it oraclegraph /opt/oracle/graph/scripts/quicksetup.sh -d

# graph visualization
http://localhost:7007/dash

# client op4j
podman exec -it oraclegraph opg4j -b http://localhost:7007

# personalized pagerank
var graph = session.readGraphByName("SALES_GRAPH", GraphSource.PG_SQL)

var analyst = session.createAnalyst()
var vertices = graph.getVertices()
var pagerank = analyst.personalizedPagerank(graph, vertices)

PgqlResultSet rs = graph.queryPgql("SELECT ID(p), p.name, p.pagerank FROM MATCH (p:PRODUCT) ON SALES_GRAPH WHERE NOT EXISTS ( SELECT * FROM MATCH (c)-[:purchased]->(p) ON SALES_GRAPH WHERE c.customer_id = 4) ORDER BY p.pagerank DESC LIMIT 3");
rs.print();
graph.publish()