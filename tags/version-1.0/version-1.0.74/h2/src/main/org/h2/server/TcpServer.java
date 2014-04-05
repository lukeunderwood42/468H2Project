/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.h2.Driver;
import org.h2.constant.SysProperties;
import org.h2.engine.Constants;
import org.h2.message.Message;
import org.h2.message.TraceSystem;
import org.h2.tools.Server;
import org.h2.util.JdbcUtils;
import org.h2.util.MathUtils;
import org.h2.util.NetUtils;

/**
 * The TCP server implements the native H2 database server protocol.
 * It supports multiple client connections to multiple databases
 * (many to many). The same database may be opened by multiple clients.
 * Also supported is the mixed mode: opening databases in embedded mode,
 * and at the same time start a TCP server to allow clients to connect to
 * the same database over the network.
 */
public class TcpServer implements Service {

    // TODO new feature: implement automatic client / server mode if 'socket'
    // file locking is used
    // TODO better exception message if the port is already in use, maybe
    // automatically use the next free port?

    /**
     * The default port to use for the TCP server.
     */
    public static final int DEFAULT_PORT = 9092;
    
    private static final int SHUTDOWN_NORMAL = 0;
    private static final int SHUTDOWN_FORCE = 1;

    private static final Map SERVERS = Collections.synchronizedMap(new HashMap());

    private int port;
    private boolean trace;
    private boolean ssl;
    private boolean stop;
    private ServerSocket serverSocket;
    private Set running = Collections.synchronizedSet(new HashSet());
    private String baseDir;
    private String url;
    private boolean allowOthers;
    private boolean ifExists;
    private Connection managementDb;
    private PreparedStatement managementDbAdd;
    private PreparedStatement managementDbRemove;
    private String managementPassword = "";
    private Thread listenerThread;
    private int nextThreadId;

    /**
     * Get the database name of the management database.
     * The management database contains a table with active sessions (SESSIONS).
     *
     * @param port the TCP server port
     * @return the database name (usually starting with mem:)
     */
    public static String getManagementDbName(int port) {
        return "mem:" + Constants.MANAGEMENT_DB_PREFIX + port;
    }

    private void initManagementDb() throws SQLException {
        Properties prop = new Properties();
        prop.setProperty("user", "sa");
        prop.setProperty("password", managementPassword);
        // avoid using the driver manager
        Connection conn = Driver.load().connect("jdbc:h2:" + getManagementDbName(port), prop);
        managementDb = conn;
        Statement stat = null;
        try {
            stat = conn.createStatement();
            stat.execute("CREATE ALIAS IF NOT EXISTS STOP_SERVER FOR \"" + TcpServer.class.getName() + ".stopServer\"");
            stat.execute("CREATE TABLE IF NOT EXISTS SESSIONS(ID INT PRIMARY KEY, URL VARCHAR, USER VARCHAR, CONNECTED TIMESTAMP)");
            managementDbAdd = conn.prepareStatement("INSERT INTO SESSIONS VALUES(?, ?, ?, NOW())");
            managementDbRemove = conn.prepareStatement("DELETE FROM SESSIONS WHERE ID=?");
        } finally {
            JdbcUtils.closeSilently(stat);
        }
        SERVERS.put("" + port, this);
    }

    /**
     * Add a connection to the management database.
     * 
     * @param id the connection id
     * @param url the database URL
     * @param user the user name
     */
    synchronized void addConnection(int id, String url, String user) {
        try {
            managementDbAdd.setInt(1, id);
            managementDbAdd.setString(2, url);
            managementDbAdd.setString(3, user);
            managementDbAdd.execute();
        } catch (SQLException e) {
            TraceSystem.traceThrowable(e);
        }
    }

    /**
     * Remove a connection from the management database.
     * 
     * @param id the connection id
     */
    synchronized void removeConnection(int id) {
        try {
            managementDbRemove.setInt(1, id);
            managementDbRemove.execute();
        } catch (SQLException e) {
            TraceSystem.traceThrowable(e);
        }
    }

    private synchronized void stopManagementDb() {
        if (managementDb != null) {
            try {
                managementDb.close();
            } catch (SQLException e) {
                TraceSystem.traceThrowable(e);
            }
            managementDb = null;
        }
    }

    public void init(String[] args) {
        port = DEFAULT_PORT;
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            if ("-trace".equals(a)) {
                trace = true;
            } else if ("-log".equals(a) && SysProperties.OLD_COMMAND_LINE_OPTIONS) {
                trace = Server.readArgBoolean(args, i) == 1;
                i++;
            } else if ("-tcpSSL".equals(a)) {
                if (Server.readArgBoolean(args, i) != 0) {
                    ssl = Server.readArgBoolean(args, i) == 1;
                    i++;
                } else {
                    ssl = true;
                }
            } else if ("-tcpPort".equals(a)) {
                port = MathUtils.decodeInt(args[++i]);
            } else if ("-tcpPassword".equals(a)) {
                managementPassword = args[++i];
            } else if ("-baseDir".equals(a)) {
                baseDir = args[++i];
            } else if ("-tcpAllowOthers".equals(a)) {
                if (Server.readArgBoolean(args, i) != 0) {
                    allowOthers = Server.readArgBoolean(args, i) == 1;
                    i++;
                } else {
                    allowOthers = true;
                }
            } else if ("-ifExists".equals(a)) {
                if (Server.readArgBoolean(args, i) != 0) {
                    ifExists = Server.readArgBoolean(args, i) == 1;
                    i++;
                } else {
                    ifExists = true;
                }
            }
        }
        org.h2.Driver.load();
        url = (ssl ? "ssl" : "tcp") + "://" + NetUtils.getLocalAddress() + ":" + port;
    }

    public String getURL() {
        return url;
    }

    /**
     * Check if this socket may connect to this server. Remote connections are
     * not allowed if the flag allowOthers is set.
     * 
     * @param socket the socket
     * @return true if this client may connect
     */
    boolean allow(Socket socket) {
        if (allowOthers) {
            return true;
        }
        return NetUtils.isLoopbackAddress(socket);
    }

    public synchronized void start() throws SQLException {
        serverSocket = NetUtils.createServerSocket(port, ssl);
        initManagementDb();
    }

    public void listen() {
        listenerThread = Thread.currentThread();
        String threadName = listenerThread.getName();
        try {
            while (!stop) {
                Socket s = serverSocket.accept();
                TcpServerThread c = new TcpServerThread(s, this, nextThreadId++);
                running.add(c);
                Thread thread = new Thread(c);
                thread.setName(threadName + " thread");
                c.setThread(thread);
                thread.start();
            }
            serverSocket = NetUtils.closeSilently(serverSocket);
        } catch (Exception e) {
            if (!stop) {
                TraceSystem.traceThrowable(e);
            }
        }
        stopManagementDb();
    }

    public synchronized boolean isRunning(boolean traceError) {
        if (serverSocket == null) {
            return false;
        }
        try {
            Socket s = NetUtils.createLoopbackSocket(port, ssl);
            s.close();
            return true;
        } catch (Exception e) {
            if (traceError) {
                traceError(e);
            }
            return false;
        }
    }

    public void stop() {
        // TODO server: share code between web and tcp servers
        // need to remove the server first, otherwise the connection is broken
        // while the server is still registered in this map
        SERVERS.remove("" + port);
        if (!stop) {
            stopManagementDb();
            stop = true;
            if (serverSocket != null) {
                try {
                    serverSocket.close();
                } catch (IOException e) {
                    TraceSystem.traceThrowable(e);
                }
                serverSocket = null;
            }
            if (listenerThread != null) {
                try {
                    listenerThread.join(1000);
                } catch (InterruptedException e) {
                    TraceSystem.traceThrowable(e);
                }
            }
        }
        // TODO server: using a boolean 'now' argument? a timeout?
        ArrayList list = new ArrayList(running);
        for (int i = 0; i < list.size(); i++) {
            TcpServerThread c = (TcpServerThread) list.get(i);
            if (c != null) {
                c.close();
                try {
                    c.getThread().join(100);
                } catch (Exception e) {
                    TraceSystem.traceThrowable(e);
                }
            }
        }
    }

    /**
     * Stop a running server. This method is called via reflection from the
     * STOP_SERVER function.
     * 
     * @param port the port where the server runs
     * @param password the password
     * @param shutdownMode the shutdown mode, SHUTDOWN_NORMAL or SHUTDOWN_FORCE.
     */
    public static void stopServer(int port, String password, int shutdownMode) {
        TcpServer server = (TcpServer) SERVERS.get("" + port);
        if (server == null) {
            return;
        }
        if (!server.managementPassword.equals(password)) {
            return;
        }
        if (shutdownMode == SHUTDOWN_NORMAL) {
            server.stopManagementDb();
            server.stop = true;
            try {
                Socket s = NetUtils.createLoopbackSocket(port, false);
                s.close();
            } catch (Exception e) {
                // try to connect - so that accept returns
            }
        } else if (shutdownMode == SHUTDOWN_FORCE) {
            server.stop();
        }
    }

    /**
     * Remove a thread from the list.
     * 
     * @param t the thread to remove
     */
    void remove(TcpServerThread t) {
        running.remove(t);
    }

    /**
     * Get the configured base directory.
     * 
     * @return the base directory
     */
    String getBaseDir() {
        return baseDir;
    }

    /**
     * Print a message if the trace flag is enabled.
     * 
     * @param s the message
     */
    void trace(String s) {
        if (trace) {
            System.out.println(s);
        }
    }
    /**
     * Print a stack trace if the trace flag is enabled.
     * 
     * @param e the exception
     */
    void traceError(Throwable e) {
        if (trace) {
            e.printStackTrace();
        }
    }

    public boolean getAllowOthers() {
        return allowOthers;
    }

    public String getType() {
        return "TCP";
    }

    public String getName() {
        return "H2 TCP Server";
    }

    boolean getIfExists() {
        return ifExists;
    }

    /**
     * Stop the TCP server with the given URL. 
     * 
     * @param url the database URL
     * @param password the password
     * @param force if the server should be stopped immediately
     */
    public static synchronized void shutdown(String url, String password, boolean force) throws SQLException {
        int port = Constants.DEFAULT_SERVER_PORT;
        int idx = url.indexOf(':', "jdbc:h2:".length());
        if (idx >= 0) {
            String p = url.substring(idx + 1);
            idx = p.indexOf('/');
            if (idx >= 0) {
                p = p.substring(0, idx);
            }
            port = MathUtils.decodeInt(p);
        }
        String db = getManagementDbName(port);
        try {
            org.h2.Driver.load();
        } catch (Throwable e) {
            throw Message.convert(e);
        }
        for (int i = 0; i < 2; i++) {
            Connection conn = null;
            PreparedStatement prep = null;
            try {
                conn = DriverManager.getConnection("jdbc:h2:" + url + "/" + db, "sa", password);
                prep = conn.prepareStatement("CALL STOP_SERVER(?, ?, ?)");
                prep.setInt(1, port);
                prep.setString(2, password);
                prep.setInt(3, force ? SHUTDOWN_FORCE : SHUTDOWN_NORMAL);
                try {
                    prep.execute();
                } catch (SQLException e) {
                    if (force) {
                        // ignore
                    } else {
                        throw e;
                    }
                }
                break;
            } catch (SQLException e) {
                if (i == 1) {
                    throw e;
                }
            } finally {
                JdbcUtils.closeSilently(prep);
                JdbcUtils.closeSilently(conn);
            }
        }
    }

    /**
     * Cancel a running statement.
     * 
     * @param sessionId the session id
     * @param statementId the statement id
     */
    void cancelStatement(String sessionId, int statementId) throws SQLException {
        ArrayList list = new ArrayList(running);
        for (int i = 0; i < list.size(); i++) {
            TcpServerThread c = (TcpServerThread) list.get(i);
            if (c != null) {
                c.cancelStatement(sessionId, statementId);
            }
        }
    }

}