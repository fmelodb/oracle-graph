import oracle.pg.rdbms.GraphServer;
import oracle.pgx.api.*;
import java.io.FileInputStream;
import java.util.Properties;

public class PersonalizedPageRank {
    public static void main(String[] args) throws Exception {
        
        Properties props = new Properties();
        props.load(new FileInputStream("env.properties"));

        String url = props.getProperty("pgx.url");
        String user = props.getProperty("pgx.username");
        String password = props.getProperty("pgx.password");

        // 1. Connect to PGX
        ServerInstance graphServer = GraphServer.getInstance(url, user,password.toCharArray());
        PgxSession session = graphServer.createSession("session_pagerank");

        // 2. Load graph (run sample_data.sql before this)
        PgxGraph graph = session.getGraph("SALES_GRAPH");

        // 3. Query which products where purchased by customer with ID 4
        PgqlResultSet rs = graph.queryPgql("SELECT id(c) as c_id, id(p) as p_id, p.name FROM MATCH (c)-[:purchased]->(p) ON SALES_GRAPH ORDER BY id(c), id(p)");
        rs.print();

        // Run Personalized PageRank
        Analyst analyst = session.createAnalyst();
        VertexSet<PgxVertex<String>> vertices = graph.getVertices();
        analyst.personalizedPagerank(graph, vertices);
        
        rs = graph.queryPgql("SELECT ID(p), p.name, p.pagerank FROM MATCH (p:PRODUCT) ON SALES_GRAPH " +
                             "WHERE NOT EXISTS ( SELECT * FROM MATCH (c)-[:purchased]->(p) ON SALES_GRAPH WHERE c.customer_id = 4) " + 
                             "ORDER BY p.pagerank DESC LIMIT 3");
        rs.print();

        session.close();
    }
}
