package com.maze.DB.Studio.util;

/**
 * Universal JDBC URL parser to extract host and database name.
 * Supports H2, MySQL, PostgreSQL, Oracle, SQL Server, MariaDB, SQLite, DB2, Sybase, SAP, Derby, jTDS SQL Server.
 */
public class JdbcUrlParser {

    /**
     * Extracts host from JDBC URL.
     * Returns "localhost" for in-memory or file-based DBs (H2, SQLite, Derby), "unknown_host" on failure.
     */
    public static String extractHost(String jdbcUrl) {
        try {
            String url = jdbcUrl.toLowerCase();

            if (url.startsWith("jdbc:h2:") || url.startsWith("jdbc:sqlite:") || url.startsWith("jdbc:derby:")) {
                return "localhost";
            }

            // Oracle: jdbc:oracle:thin:@host:1521:orcl or @host:port/service
            if (url.startsWith("jdbc:oracle:thin:@")) {
                String afterAt = url.substring(url.indexOf("@") + 1);
                if (afterAt.contains(":")) return afterAt.split(":")[0];
                return afterAt;
            }

            // SQL Server / jTDS / MySQL / MariaDB / PostgreSQL / DB2 / Sybase / SAP
            if (url.contains("//")) {
                String afterSlashes = url.substring(url.indexOf("//") + 2);

                // Stop at /, ;, or ?
                int endIdx = afterSlashes.indexOf("/");
                int semiIdx = afterSlashes.indexOf(";");
                int qIdx = afterSlashes.indexOf("?");

                endIdx = minPositive(endIdx, semiIdx, qIdx);
                if (endIdx < 0) endIdx = afterSlashes.length();

                String hostPort = afterSlashes.substring(0, endIdx);
                return hostPort.contains(":") ? hostPort.split(":")[0] : hostPort;
            }

            return "localhost";
        } catch (Exception e) {
            return "unknown_host";
        }
    }

    /**
     * Extracts database name from JDBC URL.
     * Returns "unknown_db" on failure.
     */
    public static String extractDatabaseName(String jdbcUrl) {
        try {
            String url = jdbcUrl.toLowerCase();

            // H2 memory or file: jdbc:h2:mem:testdb
            if (url.startsWith("jdbc:h2:")) {
                String[] parts = url.split(":");
                return parts[parts.length - 1].split(";")[0];
            }

            // SQLite: file path
            if (url.startsWith("jdbc:sqlite:")) {
                String[] parts = url.split(":");
                return parts[parts.length - 1].replace("/", "_");
            }

            // Derby: file or in-memory
            if (url.startsWith("jdbc:derby:")) {
                String[] parts = url.split(":");
                String db = parts[parts.length - 1].split(";")[0];
                return db.replace("/", "_");
            }

            // SQL Server / jTDS: look for databaseName=xxx
            if (url.contains("databasename=")) {
                String part = url.substring(url.indexOf("databasename=") + "databasename=".length());
                return part.contains(";") ? part.substring(0, part.indexOf(";")) : part;
            }

            // Oracle thin: jdbc:oracle:thin:@host:1521:dbname
            if (url.startsWith("jdbc:oracle:thin:@")) {
                String afterAt = url.substring(url.indexOf("@") + 1);
                if (afterAt.contains(":")) {
                    String[] parts = afterAt.split(":");
                    return parts.length >= 3 ? parts[2] : parts[parts.length - 1];
                }
                return afterAt;
            }

            // Generic format: //host:port/dbname
            if (url.contains("//")) {
                String afterSlashes = url.substring(url.indexOf("//") + 2);
                if (afterSlashes.contains("/")) {
                    String dbPart = afterSlashes.substring(afterSlashes.indexOf("/") + 1);
                    if (dbPart.contains("?")) dbPart = dbPart.substring(0, dbPart.indexOf("?"));
                    if (dbPart.contains(";")) dbPart = dbPart.substring(0, dbPart.indexOf(";"));
                    return dbPart;
                }
            }

            return null;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Helper to return the smallest positive index among multiple values.
     * Returns -1 if all are negative.
     */
    private static int minPositive(int... values) {
        int min = -1;
        for (int v : values) {
            if (v >= 0) {
                if (min < 0 || v < min) min = v;
            }
        }
        return min;
    }
}
