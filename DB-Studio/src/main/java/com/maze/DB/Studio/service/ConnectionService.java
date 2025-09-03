package com.maze.DB.Studio.service;

import com.maze.DB.Studio.model.ConnectionProfile;
import com.maze.DB.Studio.util.ResultSetUtil;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
public class ConnectionService {

    public boolean testConnection(ConnectionProfile profile) {
        try {
            Class.forName(profile.getDriverClassName());
            try (Connection c = DriverManager.getConnection(
                    profile.getJdbcUrl(),
                    profile.getUsername(),
                    profile.getPassword())) {
                return c != null && !c.isClosed();
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public List<String> listTablesOrDatabases(ConnectionProfile profile) {
        List<String> result = new ArrayList<>();
        try {
            Class.forName(profile.getDriverClassName());
            try (Connection conn = DriverManager.getConnection(
                    profile.getJdbcUrl(),
                    profile.getUsername(),
                    profile.getPassword())) {

                if (profile.getJdbcUrl().toLowerCase().contains("databasename=")) {
                    DatabaseMetaData meta = conn.getMetaData();
                    try (ResultSet rs = meta.getTables(null, null, "%", new String[]{"TABLE"})) {
                        while (rs.next()) result.add(rs.getString("TABLE_NAME"));
                    }
                } else {
                    try (ResultSet rs = conn.createStatement().executeQuery("SELECT name FROM sys.databases")) {
                        while (rs.next()) result.add(rs.getString("name"));
                    }
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
        List<String> procs = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(profile.getJdbcUrl(), profile.getUsername(), profile.getPassword())) {
            DatabaseMetaData meta = conn.getMetaData();
            try (ResultSet rs = meta.getProcedures(null, null, "%")) {
                while (rs.next()) procs.add(rs.getString("PROCEDURE_NAME"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return procs;
    }

    private List<String> getMetaDataList(ConnectionProfile profile, String type) {
        List<String> list = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(profile.getJdbcUrl(), profile.getUsername(), profile.getPassword())) {
            DatabaseMetaData meta = conn.getMetaData();
            try (ResultSet rs = meta.getTables(null, null, "%", new String[]{type})) {
                while (rs.next()) list.add(rs.getString("TABLE_NAME"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }

    public List<String> listColumns(ConnectionProfile profile, String table) {
        List<String> columns = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(profile.getJdbcUrl(), profile.getUsername(), profile.getPassword())) {
            DatabaseMetaData meta = conn.getMetaData();
            try (ResultSet rs = meta.getColumns(null, null, table, "%")) {
                while (rs.next())
                    columns.add(rs.getString("COLUMN_NAME") + " " + rs.getString("TYPE_NAME"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return columns;
    }

    /**
     * Execute query and return results as List of rows
     */
    public List<List<Object>> executeSelectQuery(ConnectionProfile profile, String sql) throws SQLException, ClassNotFoundException {
        List<List<Object>> results = new ArrayList<>();
        Class.forName(profile.getDriverClassName());
        try (Connection conn = DriverManager.getConnection(profile.getJdbcUrl(), profile.getUsername(), profile.getPassword());
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            results = ResultSetUtil.resultSetToList(rs);
        }
        return results;
    }

    /**
     * Execute update/insert/delete query
     */
    public int executeUpdateQuery(ConnectionProfile profile, String sql) throws SQLException, ClassNotFoundException {
        Class.forName(profile.getDriverClassName());
        try (Connection conn = DriverManager.getConnection(profile.getJdbcUrl(), profile.getUsername(), profile.getPassword());
             Statement stmt = conn.createStatement()) {
            return stmt.executeUpdate(sql);
        }
    }
}
