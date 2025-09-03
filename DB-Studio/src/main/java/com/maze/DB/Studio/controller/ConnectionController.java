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
        if (!model.containsAttribute("profile")) model.addAttribute("profile", new ConnectionProfile());
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

        // Only run these for JDBC (not Mongo)
        if (profile.getMongoUri() == null || profile.getMongoUri().isEmpty()) {
            model.addAttribute("views", service.listViews(profile));
            model.addAttribute("procedures", service.listStoredProcedures(profile));
        }

        return "db-home";
    }


    @PostMapping("/columns")
    public String columns(@ModelAttribute ConnectionProfile profile,
                          @RequestParam String table, Model model) {
        model.addAttribute("profile", profile);
        model.addAttribute("table", table);
        model.addAttribute("columns", service.listColumns(profile, table));
        return "columns";
    }

    @PostMapping("/query")
    public String query(@ModelAttribute ConnectionProfile profile,
                        @RequestParam String sql, Model model) {
        model.addAttribute("profile", profile);
        model.addAttribute("tables", service.listTablesOrDatabases(profile));
        try {
            if (profile.getMongoUri() != null && !profile.getMongoUri().isEmpty()) {
                model.addAttribute("results", service.executeMongoQuery(profile, sql));
            } else if (sql.trim().toLowerCase().startsWith("select")) {
                model.addAttribute("results", service.executeSelectQuery(profile, sql));
            } else {
                int affected = service.executeUpdateQuery(profile, sql);
                model.addAttribute("message", affected + " row(s) affected.");
            }
            model.addAttribute("sql", sql);
        } catch (Exception e) {
            model.addAttribute("error", "Query Error: " + e.getMessage());
        }
        return "query-editor";
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

        boolean success = false;
        try {
            if (profile.getMongoUri() != null && !profile.getMongoUri().isEmpty()) {
                // MongoDB backup
                success = service.backupMongo(profile);
            } else {
                success = service.backupJdbc(profile);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        model.addAttribute("message", success ? "Backup successful" : "Backup failed");
        return "db-home";
    }


    @PostMapping("/restore")
    public String restoreDatabase(@ModelAttribute ConnectionProfile profile, Model model) {
        model.addAttribute("profile", profile);
        model.addAttribute("tables", service.listTablesOrDatabases(profile));

        boolean success;
        if (profile.getMongoUri() != null && !profile.getMongoUri().isEmpty()) {
            // MongoDB restore
            success = service.restoreMongo(profile.getMongoUri(), "/path/to/backup/mongo");
        } else {
            // JDBC restore
            success = service.restoreJdbc(profile, "/path/to/backup/jdbc");
        }

        model.addAttribute("message", success ? "Restore successful" : "Restore failed");
        return "db-home";
    }

}
