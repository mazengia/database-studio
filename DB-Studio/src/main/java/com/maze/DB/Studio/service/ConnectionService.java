package com.maze.DB.Studio.service;

import com.maze.DB.Studio.model.ConnectionProfile;
import com.maze.DB.Studio.util.ResultSetUtil;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.*;
import lombok.RequiredArgsConstructor;
import org.bson.Document;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
public class ConnectionService {

    // ----------------- Test Connection -----------------
    public boolean testConnection(ConnectionProfile profile) {
        try {
            if (isMongo(profile)) {
                try (MongoClient client = createMongoClient(profile)) {
                    client.listDatabaseNames().first();
                }
            } else {
                Class.forName(profile.getDriverClassName());
                try (Connection conn = DriverManager.getConnection(profile.getJdbcUrl(), profile.getUsername(), profile.getPassword())) {
                    return conn != null && !conn.isClosed();
                }
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    // ----------------- List Tables / Databases -----------------
    public List<String> listTablesOrDatabases(ConnectionProfile profile) {
        List<String> result = new ArrayList<>();

        try {
            // --- Handle MongoDB first ---
            if (profile.getMongoUri() != null && !profile.getMongoUri().isEmpty()) {
                ConnectionString connString = new ConnectionString(profile.getMongoUri());
                MongoClientSettings settings = MongoClientSettings.builder()
                        .applyConnectionString(connString)
                        .build();

                try (MongoClient client = MongoClients.create(settings)) {
                    if (connString.getDatabase() == null) {
                        // List databases
                        client.listDatabaseNames().forEach(result::add);
                    } else {
                        // List collections in database
                        client.getDatabase(connString.getDatabase())
                                .listCollectionNames()
                                .forEach(result::add);
                    }
                }
                return result; // Return early for Mongo
            }

            // --- JDBC code ONLY runs if Mongo is not used ---
            Class.forName(profile.getDriverClassName());
            try (Connection conn = DriverManager.getConnection(
                    profile.getJdbcUrl(),
                    profile.getUsername(),
                    profile.getPassword())) {

                DatabaseMetaData meta = conn.getMetaData();
                try (ResultSet rs = meta.getTables(null, null, "%", new String[]{"TABLE"})) {
                    while (rs.next()) result.add(rs.getString("TABLE_NAME"));
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return result;
    }

    public List<String> listViews(ConnectionProfile profile) {
        return getMetaDataList(profile, "VIEW");
    }

    public List<String> listStoredProcedures(ConnectionProfile profile) {
        return getMetaDataProcedures(profile);
    }

    public List<String> listColumns(ConnectionProfile profile, String table) {
        List<String> columns = new ArrayList<>();
        if (isMongo(profile)) {
            try (MongoClient client = createMongoClient(profile)) {
                MongoDatabase db = client.getDatabase(new ConnectionString(profile.getMongoUri()).getDatabase());
                MongoCollection<Document> coll = db.getCollection(table);
                Document doc = coll.find().first();
                if (doc != null) columns.addAll(doc.keySet());
            }
            return columns;
        }

        try (Connection conn = DriverManager.getConnection(profile.getJdbcUrl(), profile.getUsername(), profile.getPassword())) {
            DatabaseMetaData meta = conn.getMetaData();
            try (ResultSet rs = meta.getColumns(null, null, table, "%")) {
                while (rs.next())
                    columns.add(rs.getString("COLUMN_NAME") + " " + rs.getString("TYPE_NAME"));
            }
        } catch (Exception e) { e.printStackTrace(); }
        return columns;
    }

    // ----------------- Execute SQL Queries -----------------
    public List<List<Object>> executeSelectQuery(ConnectionProfile profile, String sql) throws Exception {
        List<List<Object>> results = new ArrayList<>();
        if (isMongo(profile)) return executeMongoQuery(profile, sql);

        Class.forName(profile.getDriverClassName());
        try (Connection conn = DriverManager.getConnection(profile.getJdbcUrl(), profile.getUsername(), profile.getPassword());
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            results = ResultSetUtil.resultSetToList(rs);
        }
        return results;
    }

    public int executeUpdateQuery(ConnectionProfile profile, String sql) throws Exception {
        Class.forName(profile.getDriverClassName());
        try (Connection conn = DriverManager.getConnection(profile.getJdbcUrl(), profile.getUsername(), profile.getPassword());
             Statement stmt = conn.createStatement()) {
            return stmt.executeUpdate(sql);
        }
    }

    // ----------------- Execute Mongo Queries -----------------
    public List<List<Object>> executeMongoQuery(ConnectionProfile profile, String query) {
        List<List<Object>> results = new ArrayList<>();
        try (MongoClient client = createMongoClient(profile)) {
            MongoDatabase db = client.getDatabase(new ConnectionString(profile.getMongoUri()).getDatabase());

            // collectionName                    => fetch all
            // collectionName|{"maintainer.employeeId":"EB-1009"} => fetch with filter
            String collectionName = query;
            Document filter = new Document(); // empty filter by default

            if (query.contains("|")) {
                String[] parts = query.split("\\|", 2);
                collectionName = parts[0].trim();
                String jsonFilter = parts[1].trim();
                if (!jsonFilter.isEmpty()) filter = Document.parse(jsonFilter);
            }

            MongoCollection<Document> collection = db.getCollection(collectionName);

            for (Document doc : collection.find(filter)) {
                List<Object> row = new ArrayList<>();
                doc.forEach((k, v) -> row.add(k + "=" + v));
                results.add(row);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return results;
    }


    // ----------------- Utility -----------------
    private boolean isMongo(ConnectionProfile profile) {
        return profile.getMongoUri() != null && !profile.getMongoUri().isEmpty();
    }

    private MongoClient createMongoClient(ConnectionProfile profile) {
        ConnectionString connString = new ConnectionString(profile.getMongoUri());
        return MongoClients.create(connString);
    }

    private List<String> getMetaDataList(ConnectionProfile profile, String type) {
        List<String> list = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(profile.getJdbcUrl(), profile.getUsername(), profile.getPassword())) {
            DatabaseMetaData meta = conn.getMetaData();
            try (ResultSet rs = meta.getTables(null, null, "%", new String[]{type})) {
                while (rs.next()) list.add(rs.getString("TABLE_NAME"));
            }
        } catch (Exception e) { e.printStackTrace(); }
        return list;
    }

    private List<String> getMetaDataProcedures(ConnectionProfile profile) {
        List<String> procs = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(profile.getJdbcUrl(), profile.getUsername(), profile.getPassword())) {
            DatabaseMetaData meta = conn.getMetaData();
            try (ResultSet rs = meta.getProcedures(null, null, "%")) {
                while (rs.next()) procs.add(rs.getString("PROCEDURE_NAME"));
            }
        } catch (Exception e) { e.printStackTrace(); }
        return procs;
    }
}
