/*
    GraphREST.java depends on:

    Oracle Graph Client (23.4)
    GSON (download here: https://search.maven.org/artifact/com.google.code.gson/gson/2.10.1/jar?eh=)
    Change username, password, Graph Studio URL, Database service descriptor
    Execute: java GraphREST "pgql query"

 */


import java.net.http.*;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse.BodyHandlers;
import java.security.SecureRandom;

import com.google.gson.*;
import javax.net.ssl.*;
import java.security.cert.*;
import java.net.*;
import java.sql.*;

import java.time.Instant;
import java.time.Duration;

import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;

import oracle.pg.rdbms.pgql.PgqlConnection;
import oracle.pg.rdbms.pgql.PgqlPreparedStatement;
import oracle.pg.rdbms.pgql.PgqlResultSet;

public class GraphREST {

    public static void main(String args[]) {
        runREST(args[0]);
        runJDBC(args[0]);
    }

    public static void runJDBC(String query) {
        try {
            PoolDataSource  pds = PoolDataSourceFactory.getPoolDataSource();
            pds.setConnectionFactoryClassName("oracle.jdbc.pool.OracleDataSource");
            pds.setURL("[jdbc:oracle:thin:@service_descriptor]");
            pds.setUser("[username]");
            pds.setPassword("[password]");
            Connection conn = pds.getConnection();

            // fist run ------------
            PgqlConnection pgqlConn = PgqlConnection.getConnection(conn);            
            Instant startQuery = Instant.now();
            PgqlPreparedStatement ps = pgqlConn.prepareStatement(query);
            PgqlResultSet rs = ps.executeQuery();
            while (rs.next()) {
                rs.getString(1);                
            }
            Instant endQuery = Instant.now();
            System.out.println("PGQL JDBC Duration (ms) 1: " + Duration.between(startQuery, endQuery).toMillis());
            // ---------------------


            // second run ----------
            pgqlConn = PgqlConnection.getConnection(conn);            
            startQuery = Instant.now();
            ps = pgqlConn.prepareStatement(query);
            rs = ps.executeQuery();
            while (rs.next()) {
                rs.getString(1);                
            }
            endQuery = Instant.now();
            System.out.println("PGQL JDBC Duration (ms) 2: " + Duration.between(startQuery, endQuery).toMillis());
            // ---------------------

        } catch (Exception e) {
            System.out.println(e);
            System.exit(1);
        }
    }

    public static void runREST(String query) {
        String tokenURL = "https://localhost:7007/auth/token";
        String queryURL = "https://localhost:7007/v2/runQuery";
        String tokenData =  "{\"username\": \"[username]\",\"password\": \"[password]\"}";
        String pgql = "{ " +
                 " \"statements\": [ \"" + query + "\" ], " +
                 " \"driver\": \"PGQL_IN_DATABASE\", \"formatter\": \"GVT\", \"visualize\": true }";

        try {

            HttpClient client = HttpClient.newBuilder().build();

            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(tokenURL))
                .POST(BodyPublishers.ofString(tokenData))
                .header("Content-type", "application/json")
                .build();

            HttpResponse<String> response = client.send(request, BodyHandlers.ofString());

            JsonObject convertedObject = new Gson().fromJson(response.body(), JsonObject.class);
            String accessToken = convertedObject.get("access_token").getAsString();

            // first run ----------------------
            Instant startPGQL = Instant.now();
            request = HttpRequest.newBuilder()
                .uri(URI.create(queryURL))
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer " + accessToken)
                .POST(BodyPublishers.ofString(pgql))
                .build();
            response = client.send(request, BodyHandlers.ofString());
            Instant endPGQL = Instant.now();
            System.out.println("PGQL REST Duration (ms) 1: " + Duration.between(startPGQL, endPGQL).toMillis());
            // --------------------------------


            // second run ----------------------
            startPGQL = Instant.now();
            request = HttpRequest.newBuilder()
                .uri(URI.create(queryURL))
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer " + accessToken)
                .POST(BodyPublishers.ofString(pgql))
                .build();
            response = client.send(request, BodyHandlers.ofString());
            endPGQL = Instant.now();
            System.out.println("PGQL REST Duration (ms) 2: " + Duration.between(startPGQL, endPGQL).toMillis());
            // --------------------------------

        } catch (Exception e) {
            System.out.println(e);
            System.exit(1);
        }
    }
}