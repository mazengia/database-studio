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



---   install application on local machine
 

### 1. Build the project
Make sure you have **Maven** and **Java 21+** installed.  
Run the following commands from the project root:

```bash
mvn clean
mvn package

*. Run the application
### 2. After building, start the application with:

java -jar DB-Studio-0.0.1-SNAPSHOT.jar

### 3. Access the application

http://localhost:8080/db


Run on a different port

To use a different port (for example 9090), run:

    java -jar DB-Studio-0.0.1-SNAPSHOT.jar --server.port=9090
Then access:   http://localhost:9090/db
