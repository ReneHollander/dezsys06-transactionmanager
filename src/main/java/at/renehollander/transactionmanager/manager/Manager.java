package at.renehollander.transactionmanager.manager;

import at.renehollander.transactionmanager.Callback;
import at.renehollander.transactionmanager.Maps;
import com.corundumstudio.socketio.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Manager implements AuthorizationListener {

    private static Logger LOG = LoggerFactory.getLogger(Manager.class);

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
        LOG.info("Client " + name + " connected!");
    }

    public void execute(int timeout, String statement, Callback.NoParamsWithStringError callback) {
        Lock lock = new ReentrantLock();
        Set<SocketIOClient> clients = fetch();
        List<String> exceptions = new ArrayList<>();
        AtomicBoolean triggered = new AtomicBoolean(false);
        server.getBroadcastOperations().sendEvent("execute", Maps.of("timeout", timeout, "statement", statement), new BroadcastAckCallback<Map>(Map.class, timeout) {
            public void onClientSuccess(SocketIOClient client, Map resultMap) {
                lock.lock();
                LOG.info("Recieved response from " + client.get("name"));
                Map<String, Object> result = resultMap;
                if (result == null) result = new HashMap<>();
                clients.remove(client);
                if (result.containsKey("error")) {
                    LOG.error("client " + client.get("name") + " responded with an exception: " + result.get("error"));
                    exceptions.add("client " + client.get("name") + " responded with an exception: " + result.get("error"));
                } else {
                    LOG.info("client " + client.get("name") + " response after " + statement + " was ok");
                }
                rollbackOrCommitCheck(clients, exceptions, timeout, callback, triggered);
                lock.unlock();
            }

            public void onClientTimeout(SocketIOClient client) {
                lock.lock();
                clients.remove(client);
                LOG.error("client " + client.get("name") + " timed out");
                exceptions.add("client " + client.get("name") + " timed out");
                rollbackOrCommitCheck(clients, exceptions, timeout, callback, triggered);
                lock.unlock();
            }
        });
        clients.forEach((client) -> {
            LOG.info("Sent Statement \"" + statement + "\" to " + client.get("name"));
        });
    }

    private synchronized void rollbackOrCommitCheck(Set<SocketIOClient> clients, List<String> exceptions, int timeout, Callback.NoParamsWithStringError callback, AtomicBoolean triggered) {
        if (!triggered.get() && clients.isEmpty()) {
            triggered.set(true);
            if (!exceptions.isEmpty()) {
                LOG.info("Executing Rollback on all stations");
                rollback(timeout, callback, exceptions);
            } else if (exceptions.isEmpty()) {
                LOG.info("Executing Commit on all stations");
                commit(timeout, callback, exceptions);
            }
        }
    }

    private synchronized void callbackCheck(Set<SocketIOClient> clients, List<String> exceptions, Callback.NoParamsWithStringError callback, AtomicBoolean triggered, List<String> prev) {
        if (!triggered.get() && clients.isEmpty()) {
            triggered.set(true);
            if (exceptions.isEmpty()) {
                callback.execute(prev.isEmpty() ? null : prev.toArray(new String[prev.size()]));
            } else if (!exceptions.isEmpty()) {
                List<String> all = new ArrayList<>(exceptions);
                all.addAll(prev);
                callback.execute(all.toArray(new String[all.size()]));
            }
        }
    }

    private void rollback(int timeout, Callback.NoParamsWithStringError callback, List<String> prev) {
        Lock lock = new ReentrantLock();
        Set<SocketIOClient> clients = fetch();
        List<String> exceptions = new ArrayList<>();
        AtomicBoolean triggered = new AtomicBoolean(false);
        server.getBroadcastOperations().sendEvent("rollback", null, new BroadcastAckCallback<Map>(Map.class, timeout) {
            public void onClientSuccess(SocketIOClient client, Map resultMap) {
                lock.lock();
                Map<String, Object> result = resultMap;
                if (result == null) result = new HashMap<>();
                clients.remove(client);
                if (result.containsKey("error")) {
                    LOG.error("client " + client.get("name") + " responded with an exception while rolling back: " + result.get("error"));
                    exceptions.add("client " + client.get("name") + " responded with an exception while rolling back: " + result.get("error"));
                } else {
                    LOG.info("client " + client.get("name") + " response after rollback was ok");
                }
                callbackCheck(clients, exceptions, callback, triggered, prev);
                lock.unlock();
            }

            public void onClientTimeout(SocketIOClient client) {
                lock.lock();
                clients.remove(client);
                exceptions.add("client " + client.get("name") + " timed out while rolling back");
                LOG.error("client " + client.get("name") + " timed out while rolling back");
                callbackCheck(clients, exceptions, callback, triggered, prev);
                lock.unlock();
            }
        });
    }

    private void commit(int timeout, Callback.NoParamsWithStringError callback, List<String> prev) {
        Lock lock = new ReentrantLock();
        Set<SocketIOClient> clients = fetch();
        List<String> exceptions = new ArrayList<>();
        AtomicBoolean triggered = new AtomicBoolean(false);
        server.getBroadcastOperations().sendEvent("commit", null, new BroadcastAckCallback<Map>(Map.class, timeout) {
            public void onClientSuccess(SocketIOClient client, Map resultMap) {
                lock.lock();
                Map<String, Object> result = resultMap;
                if (result == null) result = new HashMap<>();
                clients.remove(client);
                if (result.containsKey("error")) {
                    exceptions.add("client " + client.get("name") + " responded with an exception while committing: " + result.get("error"));
                    LOG.error("client " + client.get("name") + " responded with an exception while committing: " + result.get("error"));
                } else {
                    LOG.info("client " + client.get("name") + " response after commit was ok");
                }
                callbackCheck(clients, exceptions, callback, triggered, prev);
                lock.unlock();
            }

            public void onClientTimeout(SocketIOClient client) {
                lock.lock();
                clients.remove(client);
                exceptions.add("client " + client.get("name") + " timed out while committing");
                LOG.error("client " + client.get("name") + " timed out while committing");
                callbackCheck(clients, exceptions, callback, triggered, prev);
                lock.unlock();
            }
        });
    }

    public void onDisconnect(SocketIOClient client) {
        clients.remove(client.get("name"));
        LOG.info("Client " + client.get("name") + " disconnected!");
    }

    public boolean isAuthorized(HandshakeData data) {
        String name = data.getUrlParams().get("name").get(0);
        return !(name == null || name.isEmpty()) && !clients.containsKey(name);
    }

    private Set<SocketIOClient> fetch() {
        return new HashSet<>(server.getAllClients());
    }

}
