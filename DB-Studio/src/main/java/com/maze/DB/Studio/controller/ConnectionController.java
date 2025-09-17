package com.maze.DB.Studio.controller;

import com.maze.DB.Studio.model.ConnectionProfile;
import com.maze.DB.Studio.service.ConnectionService;
import lombok.RequiredArgsConstructor;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.ByteArrayOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static com.maze.DB.Studio.util.JdbcUrlParser.extractDatabaseName;
import static com.maze.DB.Studio.util.JdbcUrlParser.extractHost;

@Controller
@RequiredArgsConstructor
@RequestMapping("/db")
public class ConnectionController {

    private final ConnectionService service;

    @GetMapping({"", "/"})
    public String home(Model model) {
        if (!model.containsAttribute("profile")) {
            model.addAttribute("profile", new ConnectionProfile());
        }
        return "connect";
    }

    @PostMapping("/connect")
    public String connect(@ModelAttribute ConnectionProfile profile, Model model) {
        try {
            // Test the connection first
            service.testConnection(profile);

            boolean mongo = isMongo(profile);
            String dbName = null;
            String serverName = null;

            if (mongo) {
                serverName = extractMongoServerName(profile.getMongoUri());
                dbName = extractMongoDatabaseName(profile.getMongoUri());
            } else {
                serverName = extractHost(profile.getJdbcUrl());
                // For JDBC URLs, attempt to extract the database name if present
                dbName = extractDatabaseName(profile.getJdbcUrl());
            }

            // Set extracted info into profile
            profile.setServerName(serverName);
            profile.setDatabaseName(dbName);
            model.addAttribute("profile", profile);
            // Determine whether to list databases or tables
            if (dbName != null &&  !dbName.isEmpty()) {
                // Database selected: list tables/collections
                model.addAttribute("tables", service.listTables(profile, dbName));
            } else {
                // No database selected: list all databases
                model.addAttribute("databases", service.listDatabases(profile));
                System.out.println("database="+model.getAttribute("database"));
            }

            // If JDBC, add views and stored procedures
            if (!mongo) {
                model.addAttribute("views", service.listViews(profile));
                model.addAttribute("procedures", service.listStoredProcedures(profile));
            }

            return "columns";

        } catch (Exception e) {
            model.addAttribute("error", service.getFriendlyErrorMessage(e, profile));
            model.addAttribute("profile", profile);
            return "connect";
        }
    }

    @PostMapping("/columns")
    public String showColumns(@ModelAttribute ConnectionProfile profile,
                              @RequestParam(required = false) String table,
                              @RequestParam(required = false) String database,
                              @RequestParam(required = false) String sql,
                              Model model) {
        model.addAttribute("profile", profile);

        // Handle Mongo separately
        if (isMongo(profile)) {
            profile.setDatabaseName(database);
            model.addAttribute("databases", service.listDatabases(profile));
        } else {
            if (database != null && !database.isBlank()) {
                profile.setDatabaseName(database);
                String jdbcUrl = profile.getJdbcUrl().trim();

                // Update JDBC URL dynamically based on database
                if (jdbcUrl.startsWith("jdbc:mysql:") || jdbcUrl.startsWith("jdbc:mariadb:")) {
                    jdbcUrl = jdbcUrl.replaceAll("/[^/?]+", "/" + database);
                } else if (jdbcUrl.startsWith("jdbc:postgresql:")) {
                    jdbcUrl = jdbcUrl.replaceAll("/[^/?]+", "/" + database);
                } else if (jdbcUrl.startsWith("jdbc:h2:")) {
                    jdbcUrl = jdbcUrl.replaceAll("([^:/]+)$", database);
                } else if (jdbcUrl.startsWith("jdbc:oracle:")) {
                    jdbcUrl = jdbcUrl.replaceAll(":([^:]+)$", ":" + database);
                } else if (jdbcUrl.startsWith("jdbc:sqlserver:") || jdbcUrl.startsWith("jdbc:jtds:sqlserver:")) {
                    if (jdbcUrl.toLowerCase().contains("databasename=")) {
                        jdbcUrl = jdbcUrl.replaceAll("(?i)databaseName=[^;]+", "databaseName=" + database);
                    } else {
                        jdbcUrl += ";databaseName=" + database;
                    }
                } else if (jdbcUrl.startsWith("jdbc:db2:")) {
                    jdbcUrl = jdbcUrl.replaceAll("/[^:]+", "/" + database);
                } else if (jdbcUrl.startsWith("jdbc:sybase:") || jdbcUrl.startsWith("jdbc:sap:")) {
                    jdbcUrl = jdbcUrl.replaceAll("/[^/?]+", "/" + database);
                } else if (jdbcUrl.startsWith("jdbc:derby:")) {
                    jdbcUrl = jdbcUrl.replaceAll("([^:;]+)$", database);
                }
                // SQLite: skip

                profile.setJdbcUrl(jdbcUrl);
                model.addAttribute("databases", service.listDatabases(profile));
            }
        }

        // List tables
        if (database != null && !database.isBlank()) {
            model.addAttribute("tables", service.listTables(profile, database));
        }

        // List columns
        if (table != null && !table.isBlank()) {
            model.addAttribute("table", table);
            model.addAttribute("tableColumns", service.listColumns(profile, table));
        }

        // Run any SQL query
        if (sql != null && !sql.trim().isEmpty()) {
            runQueryInternal(profile, sql, model);
        }

        return "columns";
    }

    public String extractMongoDatabaseName(String mongoUrl) {
        try {
            if (mongoUrl == null || mongoUrl.isEmpty()) return null;
            mongoUrl = mongoUrl.trim();
            String noOptions = mongoUrl.contains("?") ? mongoUrl.substring(0, mongoUrl.indexOf('?')) : mongoUrl;
            int lastSlash = noOptions.indexOf('/', mongoUrl.indexOf("://") + 3);
            if (lastSlash >= 0 && lastSlash + 1 < noOptions.length()) {
                String db = noOptions.substring(lastSlash + 1);
                return db.isEmpty() ? null : db;
            }
            return null;
        } catch (Exception e) {
            return null;
        }
    }

    public String extractMongoServerName(String url) {
        try {
            if (url == null || url.isEmpty()) return "Unknown";
            url = url.trim();

            if (url.startsWith("mongodb://") || url.startsWith("mongodb+srv://")) {
                String noProtocol = url.substring(url.indexOf("://") + 3);
                if (noProtocol.contains("@")) noProtocol = noProtocol.substring(noProtocol.indexOf("@") + 1);

                int endIdx = noProtocol.indexOf('/');
                if (endIdx < 0) endIdx = noProtocol.indexOf('?');
                if (endIdx < 0) endIdx = noProtocol.length();

                String hostPort = noProtocol.substring(0, endIdx);
                return hostPort.contains(":") ? hostPort.split(":")[0] : hostPort;
            }

            if (url.contains("//")) {
                String withoutPrefix = url.substring(url.indexOf("//") + 2);
                String hostPort = withoutPrefix.split("[/;]")[0];
                return hostPort.contains(":") ? hostPort.split(":")[0] : hostPort;
            }

            return "Unknown";

        } catch (Exception e) {
            return "Unknown";
        }
    }

    private boolean isMongo(ConnectionProfile profile) {
        return profile.getMongoUri() != null && !profile.getMongoUri().isEmpty();
    }

    @PostMapping("/backup")
    public String backupDatabase(@ModelAttribute ConnectionProfile profile, Model model) {
        profile.setServerName(extractHost(profile.getJdbcUrl()));
        profile.setDatabaseName(extractDatabaseName(profile.getJdbcUrl()));
        model.addAttribute("profile", profile);
        model.addAttribute("tables", service.listTablesOrDatabases(profile));
        model.addAttribute("databases", service.listDatabases(profile));

        boolean success;
        try {
            success = isMongo(profile) ? service.backupMongo(profile) : service.backupJdbc(profile);
            if (success) {
                model.addAttribute("message", "Backup successful");
                model.addAttribute("error", null);
            } else {
                model.addAttribute("error", "Backup failed");
                model.addAttribute("message", null);
            }
        } catch (Exception e) {
            model.addAttribute("error", "Backup failed: " + e.getMessage());
            model.addAttribute("message", null);
        }
        return "columns";
    }

    @PostMapping("/restore")
    public String restoreDatabase(@ModelAttribute ConnectionProfile profile,
                                  @RequestParam("file") MultipartFile file,
                                  Model model) {
        model.addAttribute("profile", profile);
        model.addAttribute("tables", service.listTablesOrDatabases(profile));
        model.addAttribute("databases", service.listDatabases(profile));

        boolean success;
        try {
            success = isMongo(profile) ? service.restoreMongo(profile, file) : service.restoreJdbc(profile, file);
            if (success) {
                model.addAttribute("message", "Restore successful");
                model.addAttribute("error", null);
            } else {
                model.addAttribute("error", "Restore failed");
                model.addAttribute("message", null);
            }
        } catch (Exception e) {
            model.addAttribute("error", "Restore failed: " + e.getMessage());
            model.addAttribute("message", null);
        }
        return "columns";
    }

    private void runQueryInternal(ConnectionProfile profile, String sql, Model model) {
        model.addAttribute("sql", sql);

        try {
            if (isMongo(profile)) {
                List<List<Object>> results = service.executeMongoQuery(profile, sql);
                model.addAttribute("results", results);
                if (!results.isEmpty()) model.addAttribute("resultColumns", results.get(0));
                return;
            }

            // Only normalize for DBs that need it (SQL Server, Oracle)
            sql = normalizeQueryForDb(profile.getJdbcUrl(), sql);

            try (Connection conn = service.getConnection(profile);
                 Statement stmt = conn.createStatement()) {

                boolean hasResultSet = stmt.execute(sql);

                if (hasResultSet) {
                    try (ResultSet rs = stmt.getResultSet()) {
                        List<List<Object>> results = new ArrayList<>();
                        ResultSetMetaData meta = rs.getMetaData();
                        int colCount = meta.getColumnCount();

                        // Header row
                        List<Object> headers = new ArrayList<>();
                        for (int i = 1; i <= colCount; i++) headers.add(meta.getColumnLabel(i));
                        results.add(headers);

                        // Data rows
                        while (rs.next()) {
                            List<Object> row = new ArrayList<>();
                            for (int i = 1; i <= colCount; i++) row.add(rs.getObject(i));
                            results.add(row);
                        }

                        model.addAttribute("results", results);
                        model.addAttribute("resultColumns", headers);
                        model.addAttribute("message", "Query executed successfully.");
                    }
                } else {
                    int updateCount = stmt.getUpdateCount();
                    model.addAttribute("message", "Query executed, " + updateCount + " row(s) affected.");
                }

            }

        } catch (Exception e) {
            model.addAttribute("error", "Query Error: " + e.getMessage());
        }
    }

    /**
     * Normalize SQL for databases that do not support LIMIT directly.
     */
    private String normalizeQueryForDb(String jdbcUrl, String sql) {
        if (jdbcUrl.startsWith("jdbc:sqlserver:") || jdbcUrl.startsWith("jdbc:oracle:")) {
            String lowerSql = sql.trim().toLowerCase();
            int limitIndex = lowerSql.lastIndexOf("limit");

            if (limitIndex != -1) {
                // Extract everything after LIMIT
                String limitPart = sql.substring(limitIndex + 5).trim();

                // Try to extract a number only
                StringBuilder num = new StringBuilder();
                for (char c : limitPart.toCharArray()) {
                    if (Character.isDigit(c)) num.append(c);
                    else break;
                }

                if (num.length() > 0) {
                    int limitValue = Integer.parseInt(num.toString());
                    String baseQuery = sql.substring(0, limitIndex).trim();

                    if (jdbcUrl.startsWith("jdbc:sqlserver:")) {
                        sql = baseQuery.replaceFirst("(?i)select", "SELECT TOP " + limitValue);
                    } else if (jdbcUrl.startsWith("jdbc:oracle:")) {
                        sql = baseQuery + " FETCH FIRST " + limitValue + " ROWS ONLY";
                    }
                } // else: ignore LIMIT if no number
            }
        }

        return sql;
    }


    @PostMapping("/generate-excel")
    public ResponseEntity<byte[]> generateExcel(@ModelAttribute ConnectionProfile profile,
                                                @RequestParam String sql) {
        try {
            List<List<Object>> results = isMongo(profile) ? service.executeMongoQuery(profile, sql)
                    : sql.trim().toLowerCase().startsWith("select") ? service.executeSelectQuery(profile, sql)
                    : null;

            if (results == null || results.isEmpty()) return ResponseEntity.noContent().build();

            Workbook workbook = new XSSFWorkbook();
            Sheet sheet = workbook.createSheet("Query Results");

            Row header = sheet.createRow(0);
            List<Object> columns = results.get(0);
            for (int i = 0; i < columns.size(); i++) {
                header.createCell(i).setCellValue(columns.get(i).toString());
            }

            for (int i = 1; i < results.size(); i++) {
                Row row = sheet.createRow(i);
                List<Object> rowData = results.get(i);
                for (int j = 0; j < rowData.size(); j++) {
                    row.createCell(j).setCellValue(rowData.get(j) != null ? rowData.get(j).toString() : "");
                }
            }

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            workbook.write(out);
            workbook.close();

            return ResponseEntity.ok()
                    .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=query-results.xlsx")
                    .contentType(MediaType.APPLICATION_OCTET_STREAM)
                    .body(out.toByteArray());

        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.internalServerError().build();
        }
    }
}
