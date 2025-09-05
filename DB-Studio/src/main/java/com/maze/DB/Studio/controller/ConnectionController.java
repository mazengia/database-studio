package com.maze.DB.Studio.controller;

import com.maze.DB.Studio.model.ConnectionProfile;
import com.maze.DB.Studio.service.ConnectionService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.List;

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
        if (!service.testConnection(profile)) {
            model.addAttribute("error", "Connection failed! Check JDBC/Mongo URI, username, driver.");
            model.addAttribute("profile", profile);
            return "connect";
        }

        model.addAttribute("profile", profile);
        model.addAttribute("tables", service.listTablesOrDatabases(profile));

        if (profile.getMongoUri() == null || profile.getMongoUri().isEmpty()) {
            model.addAttribute("views", service.listViews(profile));
            model.addAttribute("procedures", service.listStoredProcedures(profile));
        }

        return "db-home";
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
}
