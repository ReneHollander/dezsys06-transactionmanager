package at.renehollander.transactionmanager.station;

import at.renehollander.transactionmanager.Maps;
import io.socket.client.Ack;
import io.socket.client.IO;
import io.socket.client.Socket;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URISyntaxException;
import java.sql.SQLException;

public class Station {
    private static Logger LOG = LoggerFactory.getLogger(Station.class);

    private String name;

    private DatabaseConnection databaseConnection;
    private Socket socket;

    public Station(String name, File dbPath, String hostname, int port) {
        this.name = name;

        this.databaseConnection = new DatabaseConnection(new File(dbPath, name + ".db"));
        try {
            this.databaseConnection.open();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        try {
            LOG.info("Connecting to manager " + hostname + ":" + port);
            IO.Options opts = new IO.Options();
            opts.query = "name=" + getName();
            socket = IO.socket("http://" + hostname + ":" + port, opts);

            socket.on(Socket.EVENT_CONNECT, (args) -> LOG.info("Connected to manager"));
            socket.on(Socket.EVENT_CONNECT_ERROR, (args) -> LOG.info("Error connecting to manager"));
            socket.on(Socket.EVENT_CONNECT_TIMEOUT, (args) -> LOG.info("Connection Timed out"));
            socket.on(Socket.EVENT_DISCONNECT, (args) -> LOG.info("Disconnected from manager"));

            socket.on("execute", this::onExecute);
            socket.on("commit", this::onCommit);
            socket.on("rollback", this::onRollback);

            socket.connect();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private void onCommit(Object[] datas) {
        LOG.info("Recieved commit from Transaction Manager");
        Ack ack = (Ack) datas[datas.length - 1];
        getDatabaseConnection().commit((err) -> {
            if (err != null) {
                ack.call(Maps.of("error", err.getMessage()));
            } else {
                ack.call();
            }
        });
    }

    private void onRollback(Object[] datas) {
        LOG.info("Recieved rollback from Transaction Manager");
        Ack ack = (Ack) datas[datas.length - 1];
        getDatabaseConnection().rollback((err) -> {
            if (err != null) {
                ack.call(Maps.of("error", err.getMessage()));
            } else {
                ack.call();
            }
        });
    }

    public DatabaseConnection getDatabaseConnection() {
        return databaseConnection;
    }

    public void onExecute(Object... datas) {
        LOG.info("Recieved statement from Transaction Manager");
        Ack ack = (Ack) datas[datas.length - 1];
        JSONObject data = (JSONObject) datas[0];
        try {
            getDatabaseConnection().execute(data.optInt("timeout", 10), data.getString("statement"), (err, res) -> {
                if (err != null) {
                    ack.call(Maps.of("error", err.getMessage()));
                } else {
                    ack.call(Maps.of("res", res));
                }
            });
        } catch (JSONException e) {
            LOG.error("An exception occured while parsing request", e);
            ack.call(Maps.of("error", e.getMessage()));
        }
    }

    public String getName() {
        return name;
    }

    public Socket getSocket() {
        return socket;
    }

    @Override
    public String toString() {
        return "Station{" +
                "name='" + name + '\'' +
                '}';
    }
}
