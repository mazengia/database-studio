package com.maze.DB.Studio.util;

import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;


/**
 * Utility class to export a JDBC ResultSet to CSV format.
 * Supports quoting values containing commas, quotes, or newlines.
 */
public class CsvExporter {


    /**
     * Writes the given ResultSet to the provided Writer as CSV.
     * The ResultSet is fully consumed; the caller is responsible for closing it.
     */
    public static void writeResultSetToCsv(ResultSet rs, Writer writer) throws SQLException, IOException {
        ResultSetMetaData md = rs.getMetaData();
        int cols = md.getColumnCount();


// Write header row
        for (int i = 1; i <= cols; i++) {
            writer.append(escape(md.getColumnLabel(i)));
            if (i < cols) writer.append(',');
        }
        writer.append('\n');


// Write data rows
        while (rs.next()) {
            for (int i = 1; i <= cols; i++) {
                Object v = rs.getObject(i);
                writer.append(escape(v == null ? "" : v.toString()));
                if (i < cols) writer.append(',');
            }
            writer.append('\n');
        }


        writer.flush();
    }


    /**
     * Escapes a field for CSV according to RFC 4180.
     */
    private static String escape(String s) {
        if (s == null) return "";
        String out = s.replace("\"", "\"\"");
        if (out.contains(",") || out.contains("\n") || out.contains("\"")) {
            return "\"" + out + "\"";
        }
        return out;
    }
}