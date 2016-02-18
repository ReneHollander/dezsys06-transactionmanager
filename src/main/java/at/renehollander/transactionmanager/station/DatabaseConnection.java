package at.renehollander.transactionmanager.station;

import at.renehollander.transactionmanager.Callback;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class DatabaseConnection {

    private final File file;
    private Connection connection;

    public DatabaseConnection(File file) {
        this.file = file;
    }

    public Connection getConnection() {
        checkOpen();
        return connection;
    }

    public void execute(int timeout, String query, Callback.OneParamWithError<Integer> callback) {
        checkOpen();
        Thread thread = new Thread(() -> {
            try {
                Statement statement = connection.createStatement();
                statement.setQueryTimeout(timeout);
                statement.closeOnCompletion();
                int ret = statement.executeUpdate(query);
                callback.execute(ret);
            } catch (Exception e) {
                callback.execute(e);
            }

        });
        thread.start();
    }

    public void commit(Callback.NoParamsWithError callback) {
        checkOpen();
        Thread thread = new Thread(() -> {
            try {
                connection.commit();
                callback.execute(null);
            } catch (Exception e) {
                callback.execute(e);
            }

        });
        thread.start();
    }

    public void rollback(Callback.NoParamsWithError callback) {
        Thread thread = new Thread(() -> {
            try {
                connection.rollback();
                callback.execute(null);
            } catch (Exception e) {
                callback.execute(e);
            }

        });
        thread.start();
    }

    private void checkOpen() {
        if (connection == null) throw new IllegalStateException("You need to open the connection");
    }

    public void open() throws SQLException {
        if (connection == null) {
            try {
                Class.forName("org.sqlite.JDBC");
                connection = DriverManager.getConnection("jdbc:sqlite:" + file.getAbsolutePath());
                connection.setAutoCommit(false);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void close() throws SQLException {
        if (connection != null) {
            connection.close();
            connection = null;
        }
    }

}
