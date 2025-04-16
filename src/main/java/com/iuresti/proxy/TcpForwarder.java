package com.iuresti.proxy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
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
    private Socket targetSocket;

    private class ActiveConnection {
        boolean keepActiveConnection;

        void handleClient(Socket clientSocket) {
            try {
                targetSocket = new Socket(forwardHost, forwardPort);

                keepActiveConnection = true;

                logger.info("Connection: {} -> {}:{}", clientSocket.getRemoteSocketAddress(), forwardHost, forwardPort);

                executor.submit(() -> forwardData(clientSocket, targetSocket, DIRECTION.OUT));
                executor.submit(() -> forwardData(targetSocket, clientSocket, DIRECTION.IN));
            } catch (IOException ex) {
                logger.error("Client error", ex);
                keepActiveConnection = false;
            }
        }

        private void forwardData(Socket socketInput, Socket socketOutput, DIRECTION direction) {
            try (InputStream input = socketInput.getInputStream();
                 OutputStream output = socketOutput.getOutputStream()) {
                byte[] buffer = new byte[8192];
                int read;
                while (keepActive && keepActiveConnection) {
                    while ((read = input.read(buffer)) != -1) {
                        if (direction == DIRECTION.IN && blockIn) {
                            logger.info("Blocking a response of {} bytes", read);
                            continue;
                        }
                        if (direction == DIRECTION.OUT && blockOut) {
                            logger.info("Blocking a request of {} bytes", read);
                            continue;
                        }
                        output.write(buffer, 0, read);
                        output.flush();
                        logger.info("[{} -> {}:{}] {} {} bytes", listenPort, forwardHost, forwardPort, direction, read);
                    }
                    Thread.yield();
                }
            } catch (IOException ex) {
                logger.error("{} Forward error", direction, ex);
                keepActiveConnection = false;
            }

            logger.info("{} Forward terminated for listening port {}", direction, listenPort);
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

    public void stop() {
        keepActive = false;
        logger.info("TCP Forwarder stopped for port {}", listenPort);
        try {
            targetSocket.close();
        } catch (IOException e) {
            logger.error("Closing target socket", e);
        }
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
