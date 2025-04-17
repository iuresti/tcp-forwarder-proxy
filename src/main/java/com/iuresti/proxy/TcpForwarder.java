package com.iuresti.proxy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TcpForwarder {

    enum DIRECTION {
        IN, OUT
    }


    private final int listenPort;
    private final String forwardHost;
    private final int forwardPort;
    private static final Logger logger = LoggerFactory.getLogger(TcpForwarder.class);
    private boolean keepActive;
    private boolean blockIn;
    private boolean blockOut;
    private final Map<String, ActiveConnection> activeConnections = new HashMap<>();

    private class ActiveConnection {
        private final String id = UUID.randomUUID().toString();
        private Socket targetSocket;

        void handleClient(Socket clientSocket) {
            try {
                targetSocket = new Socket(forwardHost, forwardPort);

                logger.info("[{}] Connection: {} -> {}:{}", id, clientSocket.getRemoteSocketAddress(), forwardHost, forwardPort);

                executor.submit(() -> forwardData(clientSocket, targetSocket, DIRECTION.OUT));
                executor.submit(() -> forwardData(targetSocket, clientSocket, DIRECTION.IN));
            } catch (IOException ex) {
                logger.error("[{}] Client error", id, ex);
            }
        }

        private void forwardData(Socket socketInput, Socket socketOutput, DIRECTION direction) {
            try (InputStream input = socketInput.getInputStream();
                 OutputStream output = socketOutput.getOutputStream()) {
                byte[] buffer = new byte[8192];
                int read;
                while (keepActive && (read = input.read(buffer)) != -1) {
                    if (direction == DIRECTION.IN && blockIn) {
                        logger.info("[{}] Blocking a response of {} bytes", id, read);
                        continue;
                    }
                    if (direction == DIRECTION.OUT && blockOut) {
                        logger.info("[{}] Blocking a request of {} bytes", id, read);
                        continue;
                    }
                    output.write(buffer, 0, read);
                    output.flush();
                    logger.info("[{}] [{} -> {}:{}] {} {} bytes", id, listenPort, forwardHost, forwardPort, direction, read);
                }

            } catch (IOException ex) {
                logger.warn("[{}] {} Forward error: {}", id, direction, ex.getMessage());
            }

            logger.info("[{}] {} Forward terminated for listening port {}", id, direction, listenPort);
        }

        void close() {
            try {
                targetSocket.close();
            } catch (IOException e) {
                logger.error("[{}] Error closing target socket", id, e);
            }
        }
    }


    private final ExecutorService executor = Executors.newCachedThreadPool();

    public TcpForwarder(ForwardRule forwardRule) {
        this.listenPort = forwardRule.getListenPort();
        this.forwardHost = forwardRule.getForwardHost();
        this.forwardPort = forwardRule.getForwardPort();
    }

    public void start() {
        executor.submit(() -> {
            try (ServerSocket serverSocket = new ServerSocket(listenPort)) {
                logger.info("TCP Forwarder started on port {} -> {}:{}", listenPort, forwardHost, forwardPort);
                keepActive = true;
                while (!Thread.currentThread().isInterrupted() && keepActive) {
                    Socket clientSocket = serverSocket.accept();
                    ActiveConnection activeConnection = new ActiveConnection();

                    activeConnections.put(activeConnection.id, activeConnection);

                    executor.submit(() -> activeConnection.handleClient(clientSocket));
                }
            } catch (IOException ex) {
                logger.error("Listener error", ex);
            }
            logger.info("Listener {} stopped", listenPort);
        });
    }

    public boolean isKeepActive() {
        return keepActive;
    }

    public void setKeepActive(boolean keepActive) {
        this.keepActive = keepActive;
    }

    public void interruptConnection(String id) {
        if(activeConnections.containsKey(id)) {
            activeConnections.get(id).close();
            activeConnections.remove(id);
        } else {
            throw new RuntimeException("No active connection with id " + id);
        }
    }

    public void stop() {
        keepActive = false;
        logger.info("TCP Forwarder stopped for port {}", listenPort);
        activeConnections.values().forEach(ActiveConnection::close);
        activeConnections.clear();
    }

    public void blockInput() {
        logger.info("Blocking responses for port {}", listenPort);
        this.blockIn = true;
    }

    public void unblockInput() {
        logger.info("Unblocking responses for port {}", listenPort);
        this.blockIn = false;
    }

    public void blockOutput() {
        logger.info("Blocking requests for port {}", listenPort);
        this.blockOut = true;
    }

    public void unblockOutput() {
        logger.info("Unblocking requests for port {}", listenPort);
        this.blockOut = false;
    }

}
