package com.maze.DB.Studio.util;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;

public class ResultSetUtil {
    public static List<List<Object>> resultSetToList(ResultSet rs) throws SQLException {
        List<List<Object>> rows = new ArrayList<>();
        if (rs == null) return rows;

        ResultSetMetaData meta = rs.getMetaData();
        int colCount = meta.getColumnCount();

        // Header row
        List<Object> header = new ArrayList<>();
        for (int i = 1; i <= colCount; i++) header.add(meta.getColumnName(i));
        rows.add(header);

        // Data rows
        while (rs.next()) {
            List<Object> row = new ArrayList<>();
            for (int i = 1; i <= colCount; i++) row.add(rs.getObject(i));
            rows.add(row);
        }
        return rows;
    }
}
