package com.maze.DB.Studio.model;

import lombok.*;


@Data
@NoArgsConstructor
@AllArgsConstructor
public class ConnectionProfile {
    private String name;
    private String driverClassName;
    private String jdbcUrl;
    private String username;
    private String password;
    private String mongoUri;
    private String serverName;
    private String databaseName;
}
