package at.renehollander.transactionmanager.station;

import at.renehollander.transactionmanager.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class DatabaseConnection {
    private static Logger LOG = LoggerFactory.getLogger(DatabaseConnection.class);

    private final File file;
    private Connection connection;

    public DatabaseConnection(File file) {
        this.file = file;
    }

    public Connection getConnection() {
        checkOpen();
        return connection;
    }

    public void execute(int timeout, String stmt, Callback.OneParamWithError<Boolean> callback) {
        checkOpen();
        Thread thread = new Thread(() -> {
            try {
                LOG.info("Executing statement " + stmt);
                Statement statement = connection.createStatement();
                statement.setQueryTimeout(timeout);
                statement.closeOnCompletion();
                boolean ret = statement.execute(stmt);
                callback.execute(ret);
            } catch (Exception e) {
                LOG.error("An exception occured while executing statement", e);
                callback.execute(e);
            }

        });
        thread.start();
    }

    public void commit(Callback.NoParamsWithError callback) {
        checkOpen();
        Thread thread = new Thread(() -> {
            try {
                LOG.info("Comitting");
                connection.commit();
                callback.execute(null);
            } catch (Exception e) {
                LOG.error("An exception occured while committing", e);
                callback.execute(e);
            }

        });
        thread.start();
    }

    public void rollback(Callback.NoParamsWithError callback) {
        Thread thread = new Thread(() -> {
            try {
                LOG.info("Rolling Back");
                connection.rollback();
                callback.execute(null);
            } catch (Exception e) {
                LOG.error("An exception occured while rolling back", e);
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
                LOG.info("Opening db connection");
                Class.forName("org.sqlite.JDBC");
                connection = DriverManager.getConnection("jdbc:sqlite:" + file.getAbsolutePath());
                connection.setAutoCommit(false);
                connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            } catch (ClassNotFoundException e) {
                LOG.error("An exception occured while opening db connection", e);
                throw new RuntimeException(e);
            }
        }
    }

    public void close() throws SQLException {
        if (connection != null) {
            LOG.info("Closing db connection");
            connection.close();
            connection = null;
        }
    }

}
