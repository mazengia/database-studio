package com.maze.DB.Studio.model;



import jakarta.persistence.*;
import lombok.*;


@Entity
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ConnectionProfile {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String name;
    private String driverClassName;
    private String jdbcUrl;
    private String username;
    private String password;
    private String mongoUri;
}

//ConnectionProfile oracleProfile = new ConnectionProfile(
//        1L,
//        "Oracle XE",
//        "oracle.jdbc.OracleDriver",
//        "jdbc:oracle:thin:@localhost:1521/XEPDB1",
//        "HR",
//        "your_password"
//);
//
//ConnectionProfile mysqlProfile = new ConnectionProfile(
//        2L,
//        "MySQL Local",
//        "com.mysql.cj.jdbc.Driver",
//        "jdbc:mysql://localhost:3306/testdb",
//        "root",
//        "root_password"
//);
//
//ConnectionProfile postgresqlProfile = new ConnectionProfile(
//        3L,
//        "PostgreSQL Local",
//        "org.postgresql.Driver",
//        "jdbc:postgresql://localhost:5432/testdb",
//        "postgres",
//        "postgres_password"
//);
//
//ConnectionProfile sqlServerProfile = new ConnectionProfile(
//        4L,
//        "SQL Server Local",
//        "com.microsoft.sqlserver.jdbc.SQLServerDriver",
//        "jdbc:sqlserver://localhost:1433;databaseName=testdb",
//        "sa",
//        "your_password"
//);
//
//ConnectionProfile h2Profile = new ConnectionProfile(
//        5L,
//        "H2 In-Memory",
//        "org.h2.Driver",
//        "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE",
//        "sa",
//        ""
//);
//
//// Optional: MongoDB can be stored in jdbcUrl or a separate field
//ConnectionProfile mongoProfile = new ConnectionProfile(
//        6L,
//        "MongoDB Local",
//        "", // driverClassName not needed for MongoDB
//        "mongodb://username:password@localhost:27017/testdb",
//        "", // username can be inside URI
//        ""
//);
