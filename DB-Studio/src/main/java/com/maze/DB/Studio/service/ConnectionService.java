package com.maze.DB.Studio.service;

import com.maze.DB.Studio.model.ConnectionProfile;
import com.maze.DB.Studio.util.ResultSetUtil;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.*;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;

import org.bson.Document;
import org.springframework.stereotype.Service;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
public class ConnectionService {
    @Value("${db.backup.folder}")
    private String backupFolder;
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

            // Expected query format:
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
    // ---------------- Mongo Backup/Restore ----------------
    public boolean backupMongo(ConnectionProfile profile) {
        try {
            // Ensure backup folder exists
            File folder = new File(backupFolder);
            if (!folder.exists()) folder.mkdirs();

            String dbName = extractMongoDatabase(profile.getJdbcUrl());
            String backupFile = backupFolder + "/" + dbName + "_" + System.currentTimeMillis();

            // Name of your MongoDB container
            String containerName ="some-mongo"; // add this field to ConnectionProfile

            // Build docker exec command
            String command = String.format(
                    "docker exec %s mongodump -d %s -o /data/backup",
                    containerName,
                    dbName
            );

            // Execute backup
            Process process = Runtime.getRuntime().exec(command);
            int exitCode = process.waitFor();

            // Copy from container to host
            String copyCommand = String.format(
                    "docker cp %s:/data/backup/%s %s",
                    containerName,
                    dbName,
                    backupFile
            );
            Process copyProcess = Runtime.getRuntime().exec(copyCommand);
            copyProcess.waitFor();

            return exitCode == 0;

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    private String extractMongoDatabase(String mongoUri) {
        // mongodb://user:pass@host:port/db
        String[] parts = mongoUri.split("/");
        return parts[parts.length - 1].split("\\?")[0]; // remove any query params
    }


    public boolean restoreMongo(String mongoUri, String backupDir) {
        try {
            ProcessBuilder pb = new ProcessBuilder(
                    "mongorestore",
                    "--uri=" + mongoUri,
                    "--dir=" + backupDir,
                    "--drop"
            );
            Process process = pb.start();
            int exitCode = process.waitFor();
            return exitCode == 0;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    // ---------------- JDBC Backup/Restore ----------------
    public boolean backupJdbc(ConnectionProfile profile) {
        try {
            // Ensure backup folder exists
            File folder = new File(backupFolder);
            if (!folder.exists()) folder.mkdirs();

            String dbUrl = profile.getJdbcUrl(); // full JDBC URL
            String dbName = extractDatabase(dbUrl); // just "salary_upload"
            String backupFile = backupFolder + "/" + extractDatabase(dbUrl) + "_" + System.currentTimeMillis() + ".bak";

            if (dbUrl.toLowerCase().contains("sqlserver")) {
                String sql = "BACKUP DATABASE [" + dbName + "] TO DISK = N'" + backupFile + "' WITH INIT";
                Class.forName(profile.getDriverClassName());
                try (Connection conn = DriverManager.getConnection(dbUrl, profile.getUsername(), profile.getPassword());
                     Statement stmt = conn.createStatement()) {
                    stmt.execute(sql);
                }
            }
            else if (dbUrl.toLowerCase().contains("mysql")) {
                // Use mysqldump
                String command = String.format("mysqldump -u%s -p%s -h%s %s -r %s",
                        profile.getUsername(),
                        profile.getPassword(),
                        extractHost(dbUrl),
                        dbName,
                        backupFile
                );
                Process process = Runtime.getRuntime().exec(command);
                int exitCode = process.waitFor();
                return exitCode == 0;

            } else if (dbUrl.toLowerCase().contains("postgresql")) {
                // Use pg_dump
                String command = String.format("pg_dump -U %s -h %s -d %s -f %s",
                        profile.getUsername(),
                        extractHost(dbUrl),
                        dbName,
                        backupFile
                );
                Process process = Runtime.getRuntime().exec(command);
                int exitCode = process.waitFor();
                return exitCode == 0;

            } else {
                throw new UnsupportedOperationException("JDBC Backup not supported for this DB");
            }

            return true;

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }


    public boolean restoreJdbc(ConnectionProfile profile, String backupFile) {
        try {
            String command = "";
            String dbUrl = profile.getJdbcUrl();
            if (dbUrl.toLowerCase().contains("mysql")) {
                command = String.format("mysql -u%s -p%s -h%s %s < %s",
                        profile.getUsername(),
                        profile.getPassword(),
                        extractHost(dbUrl),
                        extractDatabase(dbUrl),
                        backupFile
                );
            } else if (dbUrl.toLowerCase().contains("postgresql")) {
                command = String.format("psql -U %s -h %s -d %s -f %s",
                        profile.getUsername(),
                        extractHost(dbUrl),
                        extractDatabase(dbUrl),
                        backupFile
                );
            } else {
                throw new UnsupportedOperationException("JDBC Restore not supported for this DB");
            }

            Process process = Runtime.getRuntime().exec(command);
            int exitCode = process.waitFor();
            return exitCode == 0;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    // ----------------- Utility for JDBC -----------------
    private String extractHost(String jdbcUrl) {
        // naive extraction, customize per DB type
        if (jdbcUrl.contains("@")) return jdbcUrl.split("@")[1].split(":")[0];
        return "localhost";
    }

    private String extractDatabase(String jdbcUrl) {
        if (jdbcUrl.toLowerCase().contains("databasename=")) {
            String[] parts = jdbcUrl.split(";");
            for (String part : parts) {
                if (part.toLowerCase().startsWith("databasename=")) {
                    return part.split("=")[1];
                }
            }
        }
        // fallback
        return "";
    }


}
