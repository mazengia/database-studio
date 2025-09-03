package com.maze.DB.Studio.util;

import com.maze.DB.Studio.model.ConnectionProfile;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class QueryRequest {
    private ConnectionProfile profile;
    private String sql;
}
