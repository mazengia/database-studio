package com.maze.DB.Studio.service;

import com.maze.DB.Studio.model.ConnectionProfile;
import com.maze.DB.Studio.util.ResultSetUtil;
import com.mongodb.ConnectionString;
import com.mongodb.MongoSecurityException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.client.*;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;

import org.bson.Document;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static com.maze.DB.Studio.util.JdbcUrlParser.extractDatabaseName;

@Service
@RequiredArgsConstructor
public class ConnectionService {
    @Value("${db.backup.folder}")
    private String backupFolder;
    // ----------------- Test Connection -----------------
    public void testConnection(ConnectionProfile profile) throws Exception {
        if (isMongo(profile)) {
            try (MongoClient client = createMongoClient(profile)) {
                client.listDatabaseNames().first(); // Will throw if connection fails
            } catch (MongoSecurityException e) {
                throw new Exception("MongoDB authentication failed. Please check your username and password.");
            } catch (MongoTimeoutException e) {
                throw new Exception("Cannot reach MongoDB server. Check the host, port, and network.");
            }
        } else {
            try {
                Class.forName(profile.getDriverClassName());
                try (Connection conn = DriverManager.getConnection(profile.getJdbcUrl(), profile.getUsername(), profile.getPassword())) {
                    if (conn == null || conn.isClosed()) {
                        throw new Exception("Failed to establish JDBC connection.");
                    }
                }
            } catch (SQLException e) {
                String msg = e.getMessage().toLowerCase();
                if (msg.contains("login failed") || msg.contains("access denied")) {
                    throw new Exception("Database login failed. Please check your username and password.");
                } else if (msg.contains("unknownhostexception") || msg.contains("connection refused")) {
                    throw new Exception("Cannot reach the database server. Check host, port, and network.");
                } else {
                    throw new Exception("Database connection error: " + e.getMessage());
                }
            } catch (ClassNotFoundException e) {
                throw new Exception("JDBC Driver not found. Please check your driver selection.");
            }
        }
    }

    public List<String> listTablesOrDatabases(ConnectionProfile profile) {
        List<String> result = new ArrayList<>();

        try {
            // --- MongoDB ---
            if (profile.getMongoUri() != null && !profile.getMongoUri().isEmpty()) {
                ConnectionString connString = new ConnectionString(profile.getMongoUri());
                try (MongoClient client = MongoClients.create(profile.getMongoUri())) {
                    if (connString.getDatabase() == null) {
                        // No database specified → list all databases
                        return listDatabases(profile);
                    } else {
                        // Database specified → list tables (collections)
                        return listTables(profile, connString.getDatabase());
                    }
                }
            }

            // --- JDBC ---
            Class.forName(profile.getDriverClassName());
            try (Connection conn = DriverManager.getConnection(
                    profile.getJdbcUrl(),
                    profile.getUsername(),
                    profile.getPassword())) {

                DatabaseMetaData metaData = conn.getMetaData();
                String dbProduct = metaData.getDatabaseProductName().toLowerCase();
                String catalog = conn.getCatalog(); // current database

                boolean listDatabases = false;
                if (profile.getJdbcUrl().toLowerCase().contains("databasename=")) {
                    listDatabases = false; // URL points to a database → list tables
                } else if (dbProduct.contains("sql server") || dbProduct.contains("mysql")) {
                    listDatabases = true; // SQL Server / MySQL servers → list databases
                }

                if (listDatabases) {
                    result = listDatabases(profile);
                } else {
                    result = listTables(profile, catalog);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return result;
    }

    public List<String> listDatabases(ConnectionProfile profile) {
        List<String> result = new ArrayList<>();
        try {
            if (isMongo(profile)) {
                try (MongoClient client = createMongoClient(profile)) {
                    client.listDatabaseNames().forEach(result::add);
                }
                return result;
            }

            Class.forName(profile.getDriverClassName());
            try (Connection conn = DriverManager.getConnection(
                    profile.getJdbcUrl(), profile.getUsername(), profile.getPassword())) {

                DatabaseMetaData meta = conn.getMetaData();
                String dbProduct = meta.getDatabaseProductName().toLowerCase();

                if (dbProduct.contains("sql server")) {
                    try (ResultSet rs = conn.createStatement()
                            .executeQuery("SELECT name FROM sys.databases")) {
                        while (rs.next()) result.add(rs.getString("name"));
                    }
                } else if (dbProduct.contains("mysql") || dbProduct.contains("mariadb")) {
                    try (ResultSet rs = conn.createStatement().executeQuery("SHOW DATABASES")) {
                        while (rs.next()) result.add(rs.getString(1));
                    }
                } else if (dbProduct.contains("postgresql")) {
                    try (ResultSet rs = conn.createStatement()
                            .executeQuery("SELECT datname FROM pg_database WHERE datistemplate = false")) {
                        while (rs.next()) result.add(rs.getString(1));
                    }
                } else if (dbProduct.contains("h2") || dbProduct.contains("sqlite") || dbProduct.contains("derby")) {
                    result.add(extractDatabaseName(profile.getJdbcUrl()));
                } else if (dbProduct.contains("oracle")) {
                    try (ResultSet rs = conn.createStatement()
                            .executeQuery("SELECT username FROM all_users ORDER BY username")) {
                        while (rs.next()) result.add(rs.getString("username"));
                    }
                } else if (dbProduct.contains("db2")) {
                    try (ResultSet rs = conn.createStatement()
                            .executeQuery("SELECT name FROM syscat.databases")) {
                        while (rs.next()) result.add(rs.getString("name"));
                    }
                } else if (dbProduct.contains("sybase") || dbProduct.contains("sap")) {
                    try (ResultSet rs = meta.getCatalogs()) {
                        while (rs.next()) result.add(rs.getString("TABLE_CAT"));
                    }
                } else {
                    try (ResultSet rs = meta.getCatalogs()) {
                        while (rs.next()) result.add(rs.getString("TABLE_CAT"));
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public List<String> listTables(ConnectionProfile profile, String databaseName) {
        System.out.println("profile="+profile.getJdbcUrl());
        List<String> result = new ArrayList<>();
        try {
            if (isMongo(profile)) {
                String dbName = databaseName != null ? databaseName : new ConnectionString(profile.getMongoUri()).getDatabase();
                try (MongoClient client = createMongoClient(profile)) {
                    if (dbName != null) client.getDatabase(dbName).listCollectionNames().forEach(result::add);
                }
                return result;
            }

            Class.forName(profile.getDriverClassName());
            try (Connection conn = DriverManager.getConnection(
                    profile.getJdbcUrl(), profile.getUsername(), profile.getPassword())) {

                DatabaseMetaData meta = conn.getMetaData();
                String dbProduct = meta.getDatabaseProductName().toLowerCase();
                String catalog = conn.getCatalog();
                String schema = null;

                if (dbProduct.contains("oracle") || dbProduct.contains("db2")) {
                    schema = profile.getUsername().toUpperCase();
                } else if (dbProduct.contains("postgresql")) {
                    schema = "public";
                } else if (dbProduct.contains("sql server") || dbProduct.contains("jtds:sqlserver")) {
                    schema = "dbo";
                    // Force catalog to selected database name
                    if (databaseName != null && !databaseName.isBlank()) {
                        catalog = databaseName;
                    }
                } else if (dbProduct.contains("sqlite") || dbProduct.contains("h2") || dbProduct.contains("derby")) {
                    schema = null;
                    catalog = null;
                }

                try (ResultSet rs = meta.getTables(catalog, schema, "%", new String[]{"TABLE", "VIEW"})) {
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
    public List<String> listCollections(ConnectionProfile profile, String databaseName) {
        List<String> collections = new ArrayList<>();
        try (MongoClient mongoClient = createMongoClient(profile)) {
            if (databaseName == null || databaseName.isEmpty()) {
                return collections; // No database selected
            }

            MongoDatabase database = mongoClient.getDatabase(databaseName);
            for (String name : database.listCollectionNames()) {
                collections.add(name);
            }

        } catch (Exception e) {
            e.printStackTrace(); // Or throw a custom exception
        }
        return collections;
    }

    public List<String> listCollectionFields(ConnectionProfile profile, String collectionName) {
        List<String> fields = new ArrayList<>();
        try (MongoClient mongoClient = createMongoClient(profile)) {
            String databaseName = profile.getDatabaseName();
            if (databaseName == null || databaseName.isEmpty()) {
                return fields; // No database selected
            }

            MongoDatabase database = mongoClient.getDatabase(databaseName);
            MongoCollection<Document> collection = database.getCollection(collectionName);

            // Get first document to inspect fields
            Document doc = collection.find().first();
            if (doc != null) {
                for (String key : doc.keySet()) {
                    // Optionally, include type info
                    Object value = doc.get(key);
                    String type = value != null ? value.getClass().getSimpleName() : "null";
                    fields.add(key + " " + type); // similar to "columnName columnType"
                }
            }
        } catch (Exception e) {
            e.printStackTrace(); // or throw custom exception
        }
        return fields;
    }

    public List<String> listColumns(ConnectionProfile profile, String table) {
        List<String> columns = new ArrayList<>();
        if (isMongo(profile)) {
            try (MongoClient client = createMongoClient(profile)) {
                MongoDatabase db = client.getDatabase(profile.getDatabaseName());
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
        String[] parts = mongoUri.split("/");
        return parts[parts.length - 1].split("\\?")[0]; // remove any query params
    }


    public boolean restoreMongo(ConnectionProfile profile, MultipartFile file) {
        try {
            // Save uploaded backup file to a temp folder
            File backupDir = new File("tmp/mongo_restore_" + System.currentTimeMillis());
            backupDir.mkdirs();
            File backupFile = new File(backupDir, file.getOriginalFilename());
            file.transferTo(backupFile);

            ProcessBuilder pb = new ProcessBuilder(
                    "mongorestore",
                    "--uri=" + profile.getMongoUri(),
                    "--drop",
                    "--dir=" + backupDir.getAbsolutePath()
            );
            pb.inheritIO(); // optional: shows output in console
            Process process = pb.start();
            int exitCode = process.waitFor();

            // Cleanup temp folder
            backupFile.delete();
            backupDir.delete();

            return exitCode == 0;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
    public boolean restoreJdbc(ConnectionProfile profile, MultipartFile file) {
        try {
            // Save uploaded backup file to system temp folder
            File tempDir = new File(System.getProperty("java.io.tmpdir"));
            File backupFile = new File(tempDir, "jdbc_restore_" + System.currentTimeMillis() + ".sql");
            file.transferTo(backupFile);

            String dbUrl = profile.getJdbcUrl();
            Process process;

            if (dbUrl.toLowerCase().contains("mysql")) {
                String command = String.format(
                        "mysql -u%s -p%s -h%s %s",
                        profile.getUsername(),
                        profile.getPassword(),
                        extractHost(dbUrl),
                        extractDatabase(dbUrl)
                );
                process = new ProcessBuilder("sh", "-c", command + " < " + backupFile.getAbsolutePath())
                        .inheritIO()
                        .start();

            } else if (dbUrl.toLowerCase().contains("postgresql")) {
                String command = String.format(
                        "psql -U %s -h %s -d %s -f %s",
                        profile.getUsername(),
                        extractHost(dbUrl),
                        extractDatabase(dbUrl),
                        backupFile.getAbsolutePath()
                );
                process = new ProcessBuilder("sh", "-c", command)
                        .inheritIO()
                        .start();

            }

         else if (dbUrl.toLowerCase().contains("sqlserver")) {
            String masterDbUrl = dbUrl.replace("databaseName=" + extractDatabase(dbUrl),
                    "databaseName=master");

            String dbName = extractDatabase(dbUrl);

            String sql = "ALTER DATABASE [" + dbName + "] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; " +
                    "RESTORE DATABASE [" + dbName + "] FROM DISK = N'" + backupFile.getAbsolutePath() + "' WITH REPLACE; " +
                    "ALTER DATABASE [" + dbName + "] SET MULTI_USER;";

            Class.forName(profile.getDriverClassName());
            try (Connection conn = DriverManager.getConnection(masterDbUrl, profile.getUsername(), profile.getPassword());
                 Statement stmt = conn.createStatement()) {
                stmt.execute(sql);
            }
            backupFile.delete();
            return true;
        }


        else {
                throw new UnsupportedOperationException("JDBC Restore not supported for this DB");
            }

            int exitCode = process.waitFor();
            backupFile.delete();
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



    // ----------------- Utility for JDBC -----------------
    private String extractHost(String jdbcUrl) {
        if (jdbcUrl.contains("@")) return jdbcUrl.split("@")[1].split(":")[0];
        return "localhost";
    }

    private String extractDatabase(String jdbcUrl) {
        jdbcUrl = jdbcUrl.toLowerCase();

        try {
            if (jdbcUrl.contains("sqlserver")) {
                int idx = jdbcUrl.indexOf("databasename=");
                if (idx > -1) {
                    String part = jdbcUrl.substring(idx + "databasename=".length());
                    return part.contains(";") ? part.substring(0, part.indexOf(";")) : part;
                }
            } else if (jdbcUrl.contains("mysql")) {
                // Example: jdbc:mysql://localhost:3306/salary_upload?useSSL=false
                String afterSlash = jdbcUrl.substring(jdbcUrl.indexOf("//") + 2);
                afterSlash = afterSlash.substring(afterSlash.indexOf("/") + 1);
                return afterSlash.contains("?") ? afterSlash.substring(0, afterSlash.indexOf("?")) : afterSlash;
            } else if (jdbcUrl.contains("postgresql")) {
                // Example: jdbc:postgresql://localhost:5432/salary_upload
                String afterSlash = jdbcUrl.substring(jdbcUrl.indexOf("//") + 2);
                afterSlash = afterSlash.substring(afterSlash.indexOf("/") + 1);
                return afterSlash.contains("?") ? afterSlash.substring(0, afterSlash.indexOf("?")) : afterSlash;
            } else if (jdbcUrl.contains("oracle")) {
                // Example: jdbc:oracle:thin:@localhost:1521/salary_upload
                if (jdbcUrl.contains("@")) {
                    String afterAt = jdbcUrl.substring(jdbcUrl.indexOf("@") + 1);
                    if (afterAt.contains("/")) {
                        return afterAt.substring(afterAt.indexOf("/") + 1);
                    }
                }
            }
        } catch (Exception e) {
            // fallback
        }

        return "unknown_db";
    }

    public String getFriendlyErrorMessage(Exception e, ConnectionProfile profile) {
        String msg = e.getMessage() != null ? e.getMessage().toLowerCase() : "";

        if (isMongo(profile)) {
            if (msg.contains("auth") || msg.contains("authentication")) {
                return "MongoDB authentication failed. Please check your username/password.";
            }
            if (msg.contains("host") || msg.contains("connection refused")) {
                return "Cannot reach MongoDB server. Check the host and port.";
            }
        } else {
            if (msg.contains("unknownhostexception")) {
                return "Cannot resolve database host. Check the server address.";
            }
            if (msg.contains("access denied") || msg.contains("login failed")) {
                return "Database login failed. Check your username and password.";
            }
            if (msg.contains("timeout") || msg.contains("connection refused")) {
                return "Cannot reach the database server. Check the host, port, and network.";
            }
        }
        return "Connection failed. Please verify your details and try again.";
    }
    public Connection getConnection(ConnectionProfile profile) throws SQLException {
        String jdbcUrl = profile.getJdbcUrl();
        String username = profile.getUsername();
        String password = profile.getPassword();

        try {
            // Detect driver class based on JDBC URL
            if (jdbcUrl.startsWith("jdbc:postgresql:")) {
                Class.forName("org.postgresql.Driver");
            } else if (jdbcUrl.startsWith("jdbc:mysql:") || jdbcUrl.startsWith("jdbc:mariadb:")) {
                Class.forName("com.mysql.cj.jdbc.Driver");
            } else if (jdbcUrl.startsWith("jdbc:sqlserver:")) {
                Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            } else if (jdbcUrl.startsWith("jdbc:oracle:")) {
                Class.forName("oracle.jdbc.OracleDriver");
            } else if (jdbcUrl.startsWith("jdbc:h2:")) {
                Class.forName("org.h2.Driver");
            } else if (jdbcUrl.startsWith("jdbc:sqlite:")) {
                Class.forName("org.sqlite.JDBC");
            } else if (jdbcUrl.startsWith("jdbc:db2:")) {
                Class.forName("com.ibm.db2.jcc.DB2Driver");
            } else if (jdbcUrl.startsWith("jdbc:sybase:") || jdbcUrl.startsWith("jdbc:sap:")) {
                Class.forName("com.sybase.jdbc4.jdbc.SybDriver");
            } else if (jdbcUrl.startsWith("jdbc:derby:")) {
                Class.forName("org.apache.derby.jdbc.ClientDriver");
            }
        } catch (ClassNotFoundException e) {
            throw new SQLException("JDBC Driver not found for URL: " + jdbcUrl, e);
        }

        // SQLite usually has no username/password
        if (jdbcUrl.startsWith("jdbc:sqlite:")) {
            return DriverManager.getConnection(jdbcUrl);
        }

        // Normal case: username + password
        return DriverManager.getConnection(jdbcUrl, username, password);
    }
}
