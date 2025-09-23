# Database Studio

Database Studio is a tool for exploring databases, viewing tables/collections, inspecting columns/fields, and running queries.

---

## MongoDB Usage

### 1. Get all records from a collection
To retrieve all records from a MongoDB collection, enter only the **collection name**:

Example:

    authorizelog

Then click **Run**.

---

### 2. Get records with a filter
To query records with a filter, use the following format:

    collectionName|{"field":"value"}

- The part before the `|` is the **collection name**.
- The JSON after `|` is the **filter criteria**.

Example:

    authorizelog|{"maintainer.employeeId":"EB-1009"}

Then click **Run** to execute the query.

---

## SQL / JDBC Usage

### 1. Select a database
Choose a database from the dropdown list in the top header.  
If a database is not selected, you can only browse the list of databases.

### 2. View tables
After selecting a database, the sidebar will display the list of tables.  
Click a table to view its columns.

### 3. Run SQL queries
Use the **Query Editor** to write and execute SQL queries:

```sql
SELECT * FROM employees;
```

---

## Install Application on Local Machine

### Requirements

- **Java 21 or higher** (JDK)  
  [Download Java](https://adoptium.net/temurin/releases/?version=21)
- **Maven**  
  [Download Maven](https://maven.apache.org/download.cgi)

#### Optional: Supported Database Drivers (included as dependencies)

- H2 (in-memory)
- Microsoft SQL Server
- MySQL
- Oracle
- MariaDB
- PostgreSQL
- MongoDB

### 1. Build the project

Run the following commands from the project root:

```bash
mvn clean
mvn package
```

> This will create `DB-Studio-0.0.1-SNAPSHOT.jar` in the `target/` directory.

### 2. Run the application

```bash
java -jar target/DB-Studio-0.0.1-SNAPSHOT.jar
```

### 3. Access the application

Open your browser and go to:

    http://localhost:8080/db

---

### Run on a different port

To use a different port (for example 9090), run:

```bash
java -jar target/DB-Studio-0.0.1-SNAPSHOT.jar --server.port=9090
```

Then access: [http://localhost:9090/db](http://localhost:9090/db)

---

## Troubleshooting

- **JAVA_HOME not set:**  
  Make sure your JAVA_HOME environment variable points to your Java installation.
- **Maven not installed:**  
  Run `mvn -v` to check your Maven installation.
- **Port already in use:**  
  Change the port with `--server.port=XXXX` as shown above.

---

## Project Structure

- `src/main/java` - Java source code
- `src/main/resources` - Configuration and static resources
- `pom.xml` - Maven build file

---

## Dependencies

All required dependencies (database drivers, web, validation, etc.) are declared in `pom.xml`. Maven will automatically download and install them during the build.

---

## License

This project uses the default Spring Boot project license.

---

## Contribution

Feel free to fork and submit pull requests.

---

## Contact

For issues or suggestions, please open a [GitHub Issue](https://github.com/mazengia/database-studio/issues).
