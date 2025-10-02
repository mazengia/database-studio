package com.maze.DB.Studio.service;

import com.maze.DB.Studio.model.ConnectionProfile;
import com.maze.DB.Studio.util.ResultSetUtil;
import com.mongodb.ConnectionString;
import com.mongodb.MongoSecurityException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.client.*;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import lombok.RequiredArgsConstructor;
import org.apache.poi.ss.usermodel.*;
import org.springframework.beans.factory.annotation.Value;

import org.bson.Document;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

import static com.maze.DB.Studio.util.JdbcUrlParser.extractDatabaseName;

@Service
@RequiredArgsConstructor
public class ConnectionService {
    private static final int BATCH_SIZE = 500; // Configurable for relational DBs or MongoDB
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

    public List<String> listDatabases(ConnectionProfile profile) throws Exception {
        List<String> result = new ArrayList<>();
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
                try (ResultSet rs = conn.createStatement().executeQuery("SELECT name FROM sys.databases")) {
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

        return result;
    }

    public List<String> listTables(ConnectionProfile profile, String databaseName) {
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
    // ... imports and class definition unchanged ...
    public void importExcelToTable(ConnectionProfile profile, String table, InputStream inputStream) throws Exception {
        try (Workbook workbook = WorkbookFactory.create(inputStream)) { // Auto-detect XLS/XLSX
            Sheet sheet = workbook.getSheetAt(0);
            Iterator<Row> rowIterator = sheet.iterator();

            if (!rowIterator.hasNext()) throw new IllegalArgumentException("Excel sheet is empty");

            // Read header row
            Row headerRow = rowIterator.next();
            List<String> columns = new ArrayList<>();
            for (Cell cell : headerRow) columns.add(cell.getStringCellValue().trim());

            // Read data rows
            List<List<Object>> batchRows = new ArrayList<>();
            while (rowIterator.hasNext()) {
                Row row = rowIterator.next();
                List<Object> rowData = new ArrayList<>();
                for (int i = 0; i < columns.size(); i++) {
                    Cell cell = row.getCell(i, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
                    rowData.add(getCellValue(cell));
                }
                batchRows.add(rowData);

                if (batchRows.size() >= BATCH_SIZE) {
                    bulkInsert(profile, table, columns, batchRows);
                    batchRows.clear();
                }
            }
            if (!batchRows.isEmpty()) bulkInsert(profile, table, columns, batchRows);
        }
    }

    public void importCsvToTable(ConnectionProfile profile, String table, InputStream inputStream) throws Exception {
        try (CSVReader reader = new CSVReaderBuilder(new InputStreamReader(inputStream, StandardCharsets.UTF_8)).build()) {
            List<String> columns = new ArrayList<>();
            List<List<Object>> batchRows = new ArrayList<>();
            String[] line;
            boolean headerRead = false;

            while ((line = reader.readNext()) != null) {
                if (!headerRead) {
                    columns.addAll(Arrays.asList(line));
                    headerRead = true;
                } else {
                    List<Object> rowData = Arrays.stream(line)
                            .map(String::trim)
                            .map(this::parseValue)
                            .collect(Collectors.toList());
                    batchRows.add(rowData);

                    if (batchRows.size() >= BATCH_SIZE) {
                        bulkInsert(profile, table, columns, batchRows);
                        batchRows.clear();
                    }
                }
            }
            if (!batchRows.isEmpty()) bulkInsert(profile, table, columns, batchRows);
        }
    }

    private Object getCellValue(Cell cell) {
        if (cell == null) return null;
        switch (cell.getCellType()) {
            case STRING:
                return cell.getStringCellValue().trim();
            case BOOLEAN:
                return cell.getBooleanCellValue();
            case NUMERIC:
                if (DateUtil.isCellDateFormatted(cell)) {
                    // Return java.sql.Timestamp directly
                    return new Timestamp(cell.getDateCellValue().getTime());
                }
                return cell.getNumericCellValue();
            case FORMULA:
                return cell.getCellFormula();
            case BLANK:
            default:
                return null;
        }
    }


    private Object parseValue(String value) {
        if (value == null || value.isBlank()) return null;
        try { return Integer.parseInt(value); } catch (NumberFormatException ignored) {}
        try { return Double.parseDouble(value); } catch (NumberFormatException ignored) {}
        if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false")) return Boolean.parseBoolean(value);
        return value;
    }

    private void bulkInsert(ConnectionProfile profile, String table, List<String> columns, List<List<Object>> rows) throws Exception {
        if (rows.isEmpty()) return;

        if (isMongo(profile)) {
            insertMongo(profile, table, columns, rows);
        } else {
            insertRelational(profile, table, columns, rows);
        }
    }

    private void insertMongo(ConnectionProfile profile, String table, List<String> columns, List<List<Object>> rows) {
        try (MongoClient client = createMongoClient(profile)) {
            MongoCollection<Document> collection = client.getDatabase(profile.getDatabaseName()).getCollection(table);
            List<Document> docs = new ArrayList<>();
            for (List<Object> row : rows) {
                Document doc = new Document();
                for (int i = 0; i < columns.size(); i++) doc.put(columns.get(i), i < row.size() ? row.get(i) : null);
                docs.add(doc);
            }
            if (!docs.isEmpty()) collection.insertMany(docs);
        }
    }

    private void insertRelational(ConnectionProfile profile, String table, List<String> columns, List<List<Object>> rows) throws SQLException {
        try (Connection conn = getConnection(profile)) {
            String dbProduct = conn.getMetaData().getDatabaseProductName().toLowerCase();
            String colList = columns.stream()
                    .map(c -> quoteColumn(c, dbProduct))
                    .collect(Collectors.joining(","));
            String qMarks = String.join(",", Collections.nCopies(columns.size(), "?"));
            String sql = "INSERT INTO " + table + " (" + colList + ") VALUES (" + qMarks + ")";

            Map<String, Integer> columnTypes = getColumnTypes(conn, table);

            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
                    List<Object> row = rows.get(rowIndex);
                    for (int i = 0; i < columns.size(); i++) {
                        Object val = i < row.size() ? row.get(i) : null;
                        String colName = columns.get(i);
                        Integer sqlType = columnTypes.get(colName.toLowerCase());

                        try {
                            if (val != null && (sqlType == Types.TIMESTAMP || sqlType == Types.TIMESTAMP_WITH_TIMEZONE)) {
                                if (!(val instanceof Timestamp)) {
                                    val = parseTimestamp(val.toString());
                                }
                                ps.setTimestamp(i + 1, (Timestamp) val);
                            } else {
                                ps.setObject(i + 1, val);
                            }
                        } catch (Exception e) {
                            throw new SQLException("Error at row " + (rowIndex + 2) + ", column '" + colName + "' with value '" + val + "': " + e.getMessage(), e);
                        }
                    }
                    ps.addBatch();
                }
                ps.executeBatch();
            }
        }
    }


    private Timestamp parseTimestamp(Object value) {
        if (value == null) return null;

        String str = value.toString().trim();
        if (str.isEmpty() || str.equalsIgnoreCase("NULL")) return null; // <-- handle NULL

        try {
            // Handles milliseconds
            LocalDateTime ldt = LocalDateTime.parse(str, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));
            return Timestamp.valueOf(ldt);
        } catch (Exception e1) {
            try {
                // Fallback: seconds only
                LocalDateTime ldt = LocalDateTime.parse(str, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                return Timestamp.valueOf(ldt);
            } catch (Exception e2) {
                throw new IllegalArgumentException("Invalid timestamp: " + value, e2);
            }
        }
    }

    // Utility to fetch column types from DB
    private Map<String, Integer> getColumnTypes(Connection conn, String table) throws SQLException {
        Map<String, Integer> map = new HashMap<>();
        DatabaseMetaData meta = conn.getMetaData();
        try (ResultSet rs = meta.getColumns(null, null, table, null)) {
            while (rs.next()) {
                String colName = rs.getString("COLUMN_NAME").toLowerCase();
                int sqlType = rs.getInt("DATA_TYPE"); // java.sql.Types
                map.put(colName, sqlType);
            }
        }
        return map;
    }

    private String quoteColumn(String column, String dbProduct) {
        if (dbProduct.contains("sql server")) return "[" + column + "]";
        if (dbProduct.contains("mysql") || dbProduct.contains("mariadb")) return "`" + column + "`";
        if (dbProduct.contains("postgresql")) return "\"" + column + "\"";
        return column;
    }



    public static String addPagination(String jdbcUrl, String sql, int page, int size) {
        int offset = (page - 1) * size;
        String lowerSql = sql.trim().toLowerCase();

        if (!lowerSql.startsWith("select")) return sql; // Only paginate SELECTs

        if (jdbcUrl.startsWith("jdbc:mysql:") || jdbcUrl.startsWith("jdbc:mariadb:") || jdbcUrl.startsWith("jdbc:sqlite:")) {
            return sql + " LIMIT " + size + " OFFSET " + offset;
        } else if (jdbcUrl.startsWith("jdbc:postgresql:")) {
            return sql + " LIMIT " + size + " OFFSET " + offset;
        } else if (jdbcUrl.startsWith("jdbc:sqlserver:")) {
            if (!lowerSql.contains("order by")) sql += " ORDER BY (SELECT NULL)";
            return sql + " OFFSET " + offset + " ROWS FETCH NEXT " + size + " ROWS ONLY";
        } else if (jdbcUrl.startsWith("jdbc:oracle:")) {
            return "SELECT * FROM (" + sql + ") OFFSET " + offset + " ROWS FETCH NEXT " + size + " ROWS ONLY";
        }
        return sql;
    }

    // Mongo paginated
    public List<List<Object>> executeMongoQueryPaginated(ConnectionProfile profile, String query, int page, int size) {
        List<List<Object>> results = new ArrayList<>();
        try (MongoClient client = createMongoClient(profile)) {
            MongoDatabase db = client.getDatabase(new ConnectionString(profile.getMongoUri()).getDatabase());

            String collectionName = query;
            Document filter = new Document();

            if (query.contains("|")) {
                String[] parts = query.split("\\|", 2);
                collectionName = parts[0].trim();
                String jsonFilter = parts[1].trim();
                if (!jsonFilter.isEmpty()) filter = Document.parse(jsonFilter);
            }

            MongoCollection<Document> collection = db.getCollection(collectionName);

            int offset = (page - 1) * size;
            for (Document doc : collection.find(filter).skip(offset).limit(size)) {
                List<Object> row = new ArrayList<>();
                doc.forEach((k, v) -> row.add(k + "=" + v));
                results.add(row);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return results;
    }
}
