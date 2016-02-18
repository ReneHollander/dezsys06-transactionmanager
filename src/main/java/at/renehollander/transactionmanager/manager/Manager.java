package at.renehollander.transactionmanager.manager;

import at.renehollander.transactionmanager.Callback;
import at.renehollander.transactionmanager.Maps;
import com.corundumstudio.socketio.*;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class Manager implements AuthorizationListener {

    private Map<String, SocketIOClient> clients;
    private SocketIOServer server;

    public Manager(int port) {

        clients = new HashMap<>();

        Configuration config = new Configuration();
        config.setPort(port);
        config.getSocketConfig().setReuseAddress(true);
        config.setAuthorizationListener(this);
        server = new SocketIOServer(config);

        server.addConnectListener(this::onConnect);
        server.addDisconnectListener(this::onDisconnect);

        server.start();
    }

    public void onConnect(SocketIOClient client) {
        String name = client.getHandshakeData().getUrlParams().get("name").get(0);
        client.set("name", name);
        clients.put(name, client);
        System.out.println("Client " + name + " connected!");
    }

    public void execute(int timeout, String statement, Callback.NoParamsWithError callback) {
        Set<SocketIOClient> clients = fetch();
        List<Exception> exceptions = new ArrayList<>();
        server.getBroadcastOperations().sendEvent("execute", Maps.of("timeout", timeout, "statement", statement), new BroadcastAckCallback<Map>(Map.class, timeout) {
            public void onClientSuccess(SocketIOClient client, Map resultMap) {
                Map<String, Object> result = resultMap;
                if (result == null) result = new HashMap<>();
                clients.remove(client);
                if (result.containsKey("error")) {
                    exceptions.add(new SQLException("client " + client.get("name") + " responded with an exception: " + (String) result.get("error")));
                }
                rollbackOrCommitCheck(clients, exceptions, timeout, callback);
            }

            public void onClientTimeout(SocketIOClient client) {
                clients.remove(client);
                exceptions.add(new TimeoutException("client " + client.get("name") + " timed out"));
                rollbackOrCommitCheck(clients, exceptions, timeout, callback);
            }
        });
    }

    private void rollbackOrCommitCheck(Set<SocketIOClient> clients, List<Exception> exceptions, int timeout, Callback.NoParamsWithError callback) {
        if (clients.isEmpty()) {
            if (!exceptions.isEmpty()) {
                rollback(timeout, callback);
            } else if (exceptions.isEmpty()) {
                commit(timeout, callback);
            }
        }
    }

    private void callbackCheck(Set<SocketIOClient> clients, List<Exception> exceptions, Callback.NoParamsWithError callback) {
        if (clients.isEmpty()) {
            if (exceptions.isEmpty()) {
                callback.execute(new SQLException("we needed to roll back!"));
            } else if (!exceptions.isEmpty()) {
                callback.execute(squash(exceptions));
            }
        }
    }

    private Exception squash(List<Exception> exceptions) {
        return new Exception(exceptions.stream().map(Throwable::getMessage).collect(Collectors.joining(", ")));
    }

    private void rollback(int timeout, Callback.NoParamsWithError callback) {
        Set<SocketIOClient> clients = fetch();
        List<Exception> exceptions = new ArrayList<>();
        server.getBroadcastOperations().sendEvent("rollback", null, new BroadcastAckCallback<Map>(Map.class, timeout) {
            public void onClientSuccess(SocketIOClient client, Map resultMap) {
                Map<String, Object> result = resultMap;
                if (result == null) result = new HashMap<>();
                clients.remove(client);
                if (result.containsKey("error")) {
                    exceptions.add(new SQLException("client " + client.get("name") + " responded with an exception while rolling back: " + result.get("error")));
                }
                callbackCheck(clients, exceptions, callback);
            }

            public void onClientTimeout(SocketIOClient client) {
                clients.remove(client);
                exceptions.add(new TimeoutException("client " + client.get("name") + " timed out while rolling back"));
                callbackCheck(clients, exceptions, callback);
            }
        });
    }

    private void commit(int timeout, Callback.NoParamsWithError callback) {
        Set<SocketIOClient> clients = fetch();
        List<Exception> exceptions = new ArrayList<>();
        server.getBroadcastOperations().sendEvent("commit", null, new BroadcastAckCallback<Map>(Map.class, timeout) {
            public void onClientSuccess(SocketIOClient client, Map resultMap) {
                Map<String, Object> result = resultMap;
                if (result == null) result = new HashMap<>();
                clients.remove(client);
                if (result.containsKey("error")) {
                    exceptions.add(new SQLException("client " + client.get("name") + " responded with an exception while committing: " + result.get("error")));
                }
                callbackCheck(clients, exceptions, callback);
            }

            public void onClientTimeout(SocketIOClient client) {
                clients.remove(client);
                exceptions.add(new TimeoutException("client " + client.get("name") + " timed out while committing"));
                callbackCheck(clients, exceptions, callback);
            }
        });
    }

    public void onDisconnect(SocketIOClient client) {
        clients.remove(client.get("name"));
        System.out.println("Client " + client.get("name") + " disconnected!");
    }

    public boolean isAuthorized(HandshakeData data) {
        String name = data.getUrlParams().get("name").get(0);
        return !(name == null || name.isEmpty()) && !clients.containsKey(name);
    }

    private Set<SocketIOClient> fetch() {
        return new HashSet<>(server.getAllClients());
    }

}
