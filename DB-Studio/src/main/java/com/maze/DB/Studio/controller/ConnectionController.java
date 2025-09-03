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

            // If successful, list tables/views/procedures
            model.addAttribute("profile", profile);
            model.addAttribute("tables", service.listTablesOrDatabases(profile));

            if (profile.getMongoUri() == null || profile.getMongoUri().isEmpty()) {
                model.addAttribute("views", service.listViews(profile));
                model.addAttribute("procedures", service.listStoredProcedures(profile));
            }

            return "db-home";

        } catch (Exception e) {
            // Display human-readable error to the user
            model.addAttribute("error", e.getMessage());
            model.addAttribute("profile", profile);
            return "connect";
        }
    }



    @PostMapping("/selectDatabase")
    public String selectDatabase(@ModelAttribute ConnectionProfile profile,
                                 @RequestParam String database, Model model) {
        String jdbc = profile.getJdbcUrl();
        if (jdbc.toLowerCase().contains("databasename=")) {
            jdbc = jdbc.replaceAll("(?i)databaseName=[^;]*", "databaseName=" + database);
        } else {
            jdbc += ";databaseName=" + database;
        }
        profile.setJdbcUrl(jdbc);
        model.addAttribute("profile", profile);
        model.addAttribute("tables", service.listTablesOrDatabases(profile));
        return "query-editor";
    }

    @PostMapping("/query-editor")
    public String queryEditor(@ModelAttribute ConnectionProfile profile, Model model) {
        model.addAttribute("profile", profile);
        model.addAttribute("tables", service.listTablesOrDatabases(profile));
        return "query-editor";
    }

    @PostMapping("/backup")
    public String backupDatabase(@ModelAttribute ConnectionProfile profile, Model model) {
        model.addAttribute("profile", profile);
        model.addAttribute("tables", service.listTablesOrDatabases(profile));

        boolean success = profile.getMongoUri() != null && !profile.getMongoUri().isEmpty()
                ? service.backupMongo(profile)
                : service.backupJdbc(profile);

        model.addAttribute("message", success ? "Backup successful" : "Backup failed");
        return "db-home";
    }

    @PostMapping("/restore")
    public String restoreDatabase(@ModelAttribute ConnectionProfile profile, Model model) {
        model.addAttribute("profile", profile);
        model.addAttribute("tables", service.listTablesOrDatabases(profile));

        boolean success = profile.getMongoUri() != null && !profile.getMongoUri().isEmpty()
                ? service.restoreMongo(profile.getMongoUri(), "/path/to/backup/mongo")
                : service.restoreJdbc(profile, "/path/to/backup/jdbc");

        model.addAttribute("message", success ? "Restore successful" : "Restore failed");
        return "db-home";
    }

    @PostMapping("/columns")
    public String showColumns(@ModelAttribute ConnectionProfile profile,
                              @RequestParam String table,
                              @RequestParam(required = false) String sql,
                              Model model) {
        model.addAttribute("profile", profile);
        model.addAttribute("tables", service.listTablesOrDatabases(profile));
        model.addAttribute("table", table);
        model.addAttribute("tableColumns", service.listColumns(profile, table));

        if (sql != null && !sql.trim().isEmpty()) {
            runQueryInternal(profile, sql, model);
        }

        return "columns";
    }

    @PostMapping("/query")
    public String runQuery(@ModelAttribute ConnectionProfile profile,
                           @RequestParam String sql,
                           Model model) {
        model.addAttribute("profile", profile);
        model.addAttribute("tables", service.listTablesOrDatabases(profile));
        runQueryInternal(profile, sql, model);
        return "query-editor";
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
