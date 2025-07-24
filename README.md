# Flight-Sql-Duckdb

## An [Arrow Flight SQL Server](https://arrow.apache.org/docs/format/FlightSql.html) with [DuckDB](https://duckdb.org) back-end execution engines

[<img src="https://img.shields.io/badge/dockerhub-image-green.svg?logo=Docker">](https://hub.docker.com/r/voltrondata/sqlflite)
[<img src="https://img.shields.io/badge/Documentation-dev-yellow.svg?logo=">](https://arrow.apache.org/docs/format/FlightSql.html)
[<img src="https://img.shields.io/badge/Arrow%20JDBC%20Driver-download%20artifact-red?logo=Apache%20Maven">](https://search.maven.org/search?q=a:flight-sql-jdbc-driver)
[<img src="https://img.shields.io/badge/PyPI-Arrow%20ADBC%20Flight%20SQL%20driver-blue?logo=PyPI">](https://pypi.org/project/adbc-driver-flightsql/)
[<img src="https://img.shields.io/badge/PyPI-SQLFlite%20Ibis%20Backend-blue?logo=PyPI">](https://pypi.org/project/ibis-sqlflite/)
[<img src="https://img.shields.io/badge/PyPI-SQLFlite%20SQLAlchemy%20Dialect-blue?logo=PyPI">](https://pypi.org/project/sqlalchemy-sqlflite-adbc-dialect/)
<br> Flight Sql Server/ Http Sql Server with DuckDB backend lets you run DuckDB remotely and let multiple user connect to it remotely with flight jdbc driver or over Http using arrow extension.
<br> It support all the clients including JDBC, ADBC Python flight sql driver as well as sqlflite_client CLI tool

## Dev Setup
Requirement
JDK  21

## Getting started in HTTP Mode
- Export maven options <br>
  `export MAVEN_OPTS="--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED"`
- Create a temporary directory for warehouse <br>
  `mkdir /tmp/my-warehouse`
- Start the server <br>
  `cd dazzleduck-sql-http && ../mvnw clean compile  exec:java -Dexec.mainClass="io.dazzleduck.sql.http.server.Main" -Dexec.args="--conf warehousePath=/tmp/my-warehouse"`
- On a separate terminal run a sql on server with
```
URL="http://localhost:8080/query?q=select%201"
SQL="INSTALL arrow FROM community; LOAD arrow; FROM read_arrow('/dev/stdin') SELECT count(*);"
curl -s "$URL" | duckdb -c "$SQL"
```

- Writing to Server using post <br>
```
curl -i -X POST 'http://localhost:8080/ingest?path=file1.parquet' \
  -H "Content-Type: application/vnd.apache.arrow.stream" \
  --data-binary "@example/arrow_ipc/file1.arrow"
```
- Reading the file written above <br>
```
URL="http://localhost:8080/query?q=select%20%2A%20from%20read_parquet%28%27%2Ftmp%2Fmy-warehouse%2Ffile.parquet%27%29%0A"
curl -s "$URL" | duckdb -c "$SQL"
```

### Connecting with DuckDB
```
D INSTALL arrow FROM community;
D LOAD arrow;
D SELECT * FROM read_arrow(concat('http://localhost:8080/query?q=', url_encode('select 1, 2, 3')));
```


### Enabling Authentication.
Authentication is supported with jwt. Client need to invoke login api with username/password this api would return jwt  token. This jwt token can be used for all subsequent invocation
- Export maven options<br>
  ```export MAVEN_OPTS="--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED"```
- Start the client which will display the result <br>
  ``cd dazzleduck-sql-http && ../mvnw clean compile  exec:java -Dexec.mainClass="io.dazzleduck.sql.http.server.Main" -Dexec.args="--conf auth=jwt"``
- Get the jwt token with login <br>
  ```curl -X POST 'http://localhost:8080/login' -H "Content-Type: application/json" -d '{"username": "admin", "password" : "admin"}'```
- Invoke api with jwt token
```
URL="http://localhost:8080/query?q=select%201"
curl -H "Authorization': 'Bearer <jwt-token>" -s "$URL"
```
- Run the query with jwt token by setting <br>
```
INSTALL arrow FROM community; LOAD arrow;
CREATE SECRET http_auth (
              TYPE http,
              EXTRA_HTTP_HEADERS MAP {
                 'Authorization': 'Bearer <jwt-token>'
                 }
              );
SELECT * FROM read_arrow(concat('http://localhost:8080/query?q=', url_encode('select 1, 2, 3')));
```

## Getting started with Docker in Arrow GRPC Mode
- Build the docker image with
  `./mvnw clean package -DskipTests jib:dockerBuild`
- Start the container with `example/data` mounted to the container
  ` docker run -ti -v "$PWD/example/data":/data -p 55559:55559  flight-sql-duckdb`

### Connecting to the server via JDBC
Download the [Apache Arrow Flight SQL JDBC driver](https://search.maven.org/search?q=a:flight-sql-jdbc-driver)

### Supported functionality
1. Database and schema specified as part of connection url. Passed to server as header database and schema.
2. Fetch size can be specified. It's passed to the server in header fetch_size.
3. Bulk write to parquet file using bulk upload functionality. Idea is to to bulk upload and then add those files to metadata.
4. Username and Passwords can be specified in application.conf file.

You can then use the JDBC driver to connect from your host computer to the locally running Docker Flight SQL server with this JDBC string (change the password value to match the value specified for the SQLFLITE_PASSWORD environment variable if you changed it from the example above):

# 🧩 Connecting to Apache Arrow Flight SQL with DBeaver (JDBC)

This guide walks you through how to connect **Apache Arrow Flight SQL** with **DBeaver Community Edition** using the official **Flight SQL JDBC driver**.

---

## ✅ Prerequisites

### 1. Run a Flight SQL Server

---

### 2. Install DBeaver Community Edition
Download and install DBeaver from the official site:  
👉 [https://dbeaver.io](https://dbeaver.io)

---

### 3. Download the Flight SQL JDBC Driver
Visit the Maven Central page:  
👉 [https://search.maven.org/search?q=a:flight-sql-jdbc-driver](https://search.maven.org/search?q=a:flight-sql-jdbc-driver)

> ✅ Choose the **`.jar`** file.

---

## 🚀 Setup Instructions

### 4. Launch DBeaver

Open the DBeaver application.

---

### 5. Open Driver Manager

- In the menu bar, go to **Database → Driver Manager**
  ![Driver manager menu option](/images/dbeaver_database_driver_manager_menu_option.png?raw=true "Driver manager menu option")

---

### 6. Create a New Driver

- Click the **New** button on the right  
  ![New button](/images/driver_manager_new_button.png?raw=true)

---

### 7. Add the JDBC `.jar` File

1. Click the **Libraries** tab
2. Click **Add File**
3. Select the downloaded file (e.g. `flight-sql-jdbc-driver-11.0.0.jar`)  
   ![Select jar file](/images/select_driver_jar_file.png?raw=true)
4. Click **OK** to close the driver editor

---

### 8. Configure Driver Settings

1. Go to the **Settings** tab
2. Fill in the details:
    - **Driver Name**:  
      `Apache Arrow Flight SQL`
    - **URL Template**:
      ```
      jdbc:arrow-flight-sql://{host}:{port}?database={database}&useEncryption={useEncryption}&user={user}&password={password}
      ```
    - **Driver Type**:  
      Choose `SQLite` (used here only as a placeholder)
3. Your window should look like this:  
   ![Driver Manager](/images/driver_manager_completed_window.png?raw=true)
4. Click **OK** to save
5. Click **Close** to exit Driver Manager

---

### 9. Create a New Database Connection

1. Go to **Database → New Database Connection**  
   ![New connection](/images/new_database_connection_menu_option.png?raw=true)
2. In the dialog, search:  
   ```Flight```
3. Select:  
   ```Apache Arrow Flight SQL```  
   ![Choose driver](/images/database_selection_window.png?raw=true)
4. Click **Next >**

---

### 10. Enter Connection Details

Fill in your server info:

- **Host**: `localhost`
- **Port**: `59307`
- **Username**: `admin`
- **Password**: `admin`

**Note** - if you stop/restart the Flight SQL Docker container, and attempt to connect via JDBC with the same password - you could get error: "Invalid bearer token provided. Detail: Unauthenticated".  This is because the client JDBC driver caches the bearer token signed with the previous instance's secret key.  Just change the password in the new container by changing the "SQLFLITE_PASSWORD" env var setting - and then use that to connect via JDBC.

### Connecting to the server via the new [ADBC Python Flight SQL driver](https://pypi.org/project/adbc-driver-flightsql/)

You can now use the new Apache Arrow Python ADBC Flight SQL driver to query the Flight SQL server.  ADBC offers performance advantages over JDBC - because it minimizes serialization/deserialization, and data stays in columnar format at all phases.

You can learn more about ADBC and Flight SQL [here](https://voltrondata.com/resources/simplifying-database-connectivity-with-arrow-flight-sql-and-adbc).

Ensure you have Python 3.9+ installed, then open a terminal, then run:
```bash
# Create a Python virtual environment
python3 -m venv .venv

# Activate the virtual environment
. .venv/bin/activate

# Install the requirements including the new Arrow ADBC Flight SQL driver
pip install --upgrade pip
pip install pandas pyarrow adbc_driver_flightsql

# Start the python interactive shell
python
```

In the Python shell - you can then run:
```python
import os
from adbc_driver_flightsql import dbapi as sqlflite, DatabaseOptions


with sqlflite.connect(uri="grpc+tls://localhost:55559",
                        db_kwargs={"username": os.getenv("SQLFLITE_USERNAME", "admin"),
                                   "password": os.getenv("SQLFLITE_PASSWORD", "admin"),
                                   DatabaseOptions.TLS_SKIP_VERIFY.value: "true"  # Not needed if you use a trusted CA-signed TLS cert
                                   }
                        ) as conn:
   with conn.cursor() as cur:
       cur.execute("select * from generate_series(20)",
                   )
       x = cur.fetch_arrow_table()
       print(x)
```

You should see results:


### Connecting via [Ibis](https://ibis-project.org)
See: https://github.com/ibis-project/ibis-sqlflite

### Connecting via [SQLAlchemy](https://www.sqlalchemy.org)
See: https://github.com/prmoore77/sqlalchemy-sqlflite-adbc-dialect
