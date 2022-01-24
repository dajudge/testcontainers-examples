package com.dajudge.testcontainers.examples;

import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.*;
import java.util.Properties;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PostgreSQLTests {
    private static final String DATABASE_NAME = "mydb";
    private static final String SCHEMA_NAME = "myschema";

    @ClassRule
    public static final PostgreSQLContainer<?> PSQL = new PostgreSQLContainer<>("postgres:9.6.12")
            .withDatabaseName(DATABASE_NAME);

    @Test
    public void check_database_name() throws SQLException {
        withConnection(conn -> {
            try (final PreparedStatement statement = conn.prepareStatement("SELECT current_database()")) {
                try (final ResultSet resultSet = statement.executeQuery()) {
                    assertTrue(resultSet.next());
                    assertEquals(DATABASE_NAME, resultSet.getString(1));
                }
            }
        });
    }

    @Test
    public void work_with_custom_schema() throws SQLException {
        withConnection(conn -> {
            try (final PreparedStatement statement = conn.prepareStatement(format("CREATE SCHEMA %s", SCHEMA_NAME))) {
                statement.execute();
            }
            try (final PreparedStatement statement = conn.prepareStatement("CREATE TABLE test(ID INT PRIMARY KEY)")) {
                statement.execute();
            }
            try (final PreparedStatement statement = conn.prepareStatement(format("SELECT * FROM %s.test", SCHEMA_NAME))) {
                statement.executeQuery().close();
            }
            try (final PreparedStatement statement = conn.prepareStatement("SELECT * FROM test")) {
                statement.executeQuery().close();
            }
        });
    }

    private void withConnection(final JDBCConsumer<Connection> consumer) throws SQLException {
        final Properties properties = new Properties();
        properties.setProperty("user", PSQL.getUsername());
        properties.setProperty("password", PSQL.getPassword());
        properties.setProperty("currentSchema", SCHEMA_NAME);
        try (final Connection conn = DriverManager.getConnection(PSQL.getJdbcUrl(), properties)) {
            consumer.accept(conn);
        }
    }

    private interface JDBCConsumer<T> {
        void accept(T t) throws SQLException;
    }
}
