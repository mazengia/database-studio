package com.maze.DB.Studio.controller;

import com.maze.DB.Studio.model.ConnectionProfile;
import com.maze.DB.Studio.service.ConnectionService;
import lombok.RequiredArgsConstructor;
import org.apache.poi.ss.usermodel.*;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.io.ByteArrayOutputStream;
import java.util.List;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.multipart.MultipartFile;

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
            service.testConnection(profile);
            profile.setServerName(extractServerName(profile.getJdbcUrl()));
            model.addAttribute("profile", profile);
            if (profile.getJdbcUrl().toLowerCase().contains("databasename=")) {
                profile.setDatabaseName(extractDatabaseName(profile.getJdbcUrl()));
                model.addAttribute("tables", service.listTablesOrDatabases(profile));
            }
            else{
                model.addAttribute("databases", service.listTablesOrDatabases(profile));
            }

            if (profile.getMongoUri() == null || profile.getMongoUri().isEmpty()) {
                model.addAttribute("views", service.listViews(profile));
                model.addAttribute("procedures", service.listStoredProcedures(profile));
            }

            return "columns";
        } catch (Exception e) {
            model.addAttribute("error", e.getMessage());
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

        profile.setDatabaseName(database);
        model.addAttribute("profile", profile);
        // Populate tables if database selected
        if (database != null && !database.isBlank()) {
            if (!profile.getJdbcUrl().toLowerCase().contains("databasename=")) {
                profile.setJdbcUrl(profile.getJdbcUrl() + ";databaseName=" + database);
            }
            model.addAttribute("tables", service.listTables(profile, database));
        }

        // Populate columns if table selected
        if (table != null && !table.isBlank()) {
            model.addAttribute("table", table);
            model.addAttribute("tableColumns", service.listColumns(profile, table));
        }

        // Run query if provided
        if (sql != null && !sql.trim().isEmpty()) {
            runQueryInternal(profile, sql, model); // populates "results"
        }

        return "columns";
    }


    public String extractServerName(String jdbcUrl) {
        try {
            String withoutPrefix = jdbcUrl.substring(jdbcUrl.indexOf("//") + 2);
            String hostPort = withoutPrefix.split("[/;]")[0];
            return hostPort.contains(":") ? hostPort.split(":")[0] : hostPort;
        } catch (Exception e) {
            return "Unknown";
        }
    }

    public String extractDatabaseName(String jdbcUrl) {
        try {
            String[] parts = jdbcUrl.split(";");
            for (String part : parts) {
                if (part.trim().toLowerCase().startsWith("databasename=")) {
                    return part.split("=", 2)[1];
                }
            }
            return "Unknown";
        } catch (Exception e) {
            return "Unknown";
        }
    }



    @PostMapping("/backup")
    public String backupDatabase(@ModelAttribute ConnectionProfile profile, Model model) {

        profile.setServerName(extractServerName(profile.getJdbcUrl()));
        profile.setDatabaseName(extractDatabaseName(profile.getJdbcUrl()));
        model.addAttribute("profile", profile);
        model.addAttribute("tables", service.listTablesOrDatabases(profile));

        boolean success;
        try {
            if (profile.getMongoUri() != null && !profile.getMongoUri().isEmpty()) {
                success = service.backupMongo(profile);
            } else {
                success = service.backupJdbc(profile);
            }
            if (success) {
                model.addAttribute("message", "Backup successful");
                model.addAttribute("error", null);
            } else {
                model.addAttribute("error", "Backup failed");
                model.addAttribute("message", null);
            }
        } catch (Exception e) {
            e.printStackTrace();
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

        boolean success;
        try {
            if (profile.getMongoUri() != null && !profile.getMongoUri().isEmpty()) {
                success = service.restoreMongo(profile, file);
            } else {
                success = service.restoreJdbc(profile, file);
            }
            if (success) {
                model.addAttribute("message", "Restore successful");
                model.addAttribute("error", null);
            } else {
                model.addAttribute("error", "Restore failed");
                model.addAttribute("message", null);
            }
        } catch (Exception e) {
            e.printStackTrace();
            model.addAttribute("error", "Restore failed: " + e.getMessage());
            model.addAttribute("message", null);
        }

        return "columns";
    }

    // Internal method to avoid duplicate code
    private void runQueryInternal(ConnectionProfile profile, String sql, Model model) {
        model.addAttribute("sql", sql);
        try {
            List<List<Object>> results;
            if (profile.getMongoUri() != null && !profile.getMongoUri().isEmpty()) {
                results = service.executeMongoQuery(profile, sql);
            } else if (sql.trim().toLowerCase().startsWith("select")) {
                results = service.executeSelectQuery(profile, sql);
            } else {
                int affected = service.executeUpdateQuery(profile, sql);
                model.addAttribute("message", affected + " row(s) affected.");
                return;
            }
            model.addAttribute("results", results);
            if (!results.isEmpty()) model.addAttribute("resultColumns", results.get(0));
        } catch (Exception e) {
            model.addAttribute("error", "Query Error: " + e.getMessage());
        }
    }
    @PostMapping("/generate-excel")
    public ResponseEntity<byte[]> generateExcel(@ModelAttribute ConnectionProfile profile,
                                                @RequestParam String sql) {
        try {
            List<List<Object>> results;
            if (profile.getMongoUri() != null && !profile.getMongoUri().isEmpty()) {
                results = service.executeMongoQuery(profile, sql);
            } else if (sql.trim().toLowerCase().startsWith("select")) {
                results = service.executeSelectQuery(profile, sql);
            } else {
                return ResponseEntity.badRequest().body(null);
            }

            if (results.isEmpty()) {
                return ResponseEntity.noContent().build();
            }

            Workbook workbook = new XSSFWorkbook();
            Sheet sheet = workbook.createSheet("Query Results");

            // Header row
            Row header = sheet.createRow(0);
            List<Object> columns = results.get(0);
            for (int i = 0; i < columns.size(); i++) {
                Cell cell = header.createCell(i);
                cell.setCellValue(columns.get(i).toString());
            }

            // Data rows
            for (int i = 1; i < results.size(); i++) {
                Row row = sheet.createRow(i);
                List<Object> rowData = results.get(i);
                for (int j = 0; j < rowData.size(); j++) {
                    Cell cell = row.createCell(j);
                    cell.setCellValue(rowData.get(j) != null ? rowData.get(j).toString() : "");
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
