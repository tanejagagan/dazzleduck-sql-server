package io.dazzleduck.sql.flight.server;


import org.apache.arrow.driver.jdbc.ArrowFlightConnection;
import org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver;
import org.apache.arrow.driver.jdbc.client.ArrowFlightSqlClientHandler;
import org.apache.arrow.flight.FlightServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class DuckDBFlightJDBCTest {

    private static final String LONG_RUNNING_QUERY = "with t as " +
            "(select len(split(concat('abcdefghijklmnopqrstuvwxyz:', generate_series), ':')) as len  from generate_series(1, 1000000000) )" +
            " select count(*) from t where len = 10";
    private static FlightServer flightServer ;
    private static final int port = 55556;
    static String url = String.format("jdbc:arrow-flight-sql://localhost:%s/?database=memory&useEncryption=0&user=admin&password=admin", port);
    @BeforeAll
    public static void beforeAll() throws IOException, NoSuchAlgorithmException {
        String[] args = {"--conf", "port=" + port, "--conf", "useEncryption=false"};
        flightServer = Main.createServer(args);
        Thread severThread = new Thread(() -> {
            try {
                flightServer.start();
                System.out.println("S1: Server (Location): Listening on port " + flightServer.getPort());
                flightServer.awaitTermination();
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        severThread.start();
    }

    @AfterAll
    public static void afterAll() throws InterruptedException {
        flightServer.close();
    }



    private static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(url);
    }

    @Test
    public void testExecuteQuery() throws SQLException {
        try(Connection connection = getConnection();
            Statement  st = connection.createStatement()) {
            st.executeQuery("select 1");
            try ( ResultSet resultSet = st.getResultSet()) {
                while (resultSet.next()){
                    resultSet.getInt(1);
                }
            }
        }
    }

    @Test
    public void cancelBeforeExecute() throws SQLException {
        try(Connection connection = getConnection();
            Statement  st = connection.createStatement()) {
            st.cancel();
        }
    }

    @Test
    public void testExecuteQueryWithUserPassword() throws SQLException {
        var numIterator = 1;
        try(Connection connection = getConnection()) {
            for (int i = 0 ; i < numIterator; i ++) {
                try( Statement  st = connection.createStatement()) {
                    st.executeQuery("select 1");
                    try (ResultSet resultSet = st.getResultSet()) {
                        while (resultSet.next()) {
                            resultSet.getInt(1);
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testExecuteQueryWithClientHandler() throws Exception {
        var connection = getConnection(url, new Properties());
        var handler = getPrivateClientHandlerFromConnection(connection);
        for(int i = 0 ; i < 3 ; i ++) {
            var info = handler.getInfo("select 1");
            assertNotNull(info);
        }
    }

    @Test
    @Disabled
    public void cancelAfterExecute() throws SQLException {
        try(Connection connection = getConnection();
            Statement  st = connection.createStatement()) {
            Thread cancelThread = new Thread(() -> {
                try {
                    // Cancel after 1 second.
                    Thread.sleep(1000);
                    st.cancel();
                } catch (SQLException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            cancelThread.start();
            st.execute(LONG_RUNNING_QUERY);
            st.cancel();
        }
    }


    @Test
    @Disabled
    public void testSetUnSet() throws SQLException {
        try(Connection connection = getConnection();
            Statement  st = connection.createStatement()) {
            st.executeUpdate("set pivot_limit=9999");
            st.execute("SELECT current_setting('pivot_limit') AS memlimit");
            try(ResultSet set  = st.getResultSet()) {
                while (set.next()){
                    System.out.println(set.getObject(1, Long.class));
                }
            }
        }
    }

    @Test
    // These are disabled because the issue with driver which
    // does not let me troubleshoot the issue as it's a uber jar
    // and lines do not match.
    public void testMetadata() throws SQLException {
        try(Connection connection = getConnection()) {
            var databaseMetadata = connection.getMetaData();
            //databaseMetadata.getTables();
            try (var rs = databaseMetadata.getSchemas()) {
                while(rs.next()) {
                    System.out.printf("%s, %s\n",
                            rs.getString(1),
                            rs.getString(2));
                }
            }

            //databaseMetadata.getCatalogs();
            try (var rs = databaseMetadata.getCatalogs()) {
                while(rs.next()) {
                    System.out.printf("%s\n",
                            rs.getString(1));
                }
            }

            try( var rs = databaseMetadata.getTables(null, null, null, null)){
                while(rs.next()) {
                    System.out.printf("%s",
                            rs.getString(1));
                }
            }
        }
    }


    private static ArrowFlightConnection getConnection(String url, Properties properties) throws SQLException {
        return new ArrowFlightJdbcDriver().connect(url, properties);
    }
    private static ArrowFlightSqlClientHandler getPrivateClientHandlerFromConnection(ArrowFlightConnection connection) throws NoSuchFieldException, IllegalAccessException {
        // The class and field name are the same
        Field clientHandler = ArrowFlightConnection.class.getDeclaredField("clientHandler");
        clientHandler.setAccessible(true);
        // Cast to the real class
        return (ArrowFlightSqlClientHandler) clientHandler.get(connection);
    }
}