package com.taobao.tddl.dbsync.binlog;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.SocketTimeoutException;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * TODO: Document It!!
 * 
 * <pre>
 * DirectLogFetcher fetcher = new DirectLogFetcher();
 * fetcher.open(conn, file, 0, 13);
 * 
 * while (fetcher.fetch()) {
 *     LogEvent event;
 *     do {
 *         event = decoder.decode(fetcher, context);
 * 
 *         // process log event.
 *     } while (event != null);
 * }
 * // connection closed.
 * </pre>
 * 
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
 * @version 1.0
 */
public final class DirectLogFetcher extends LogFetcher {

    protected static final Log logger                          = LogFactory.getLog(DirectLogFetcher.class);

    /** Command to dump binlog */
    public static final byte   COM_BINLOG_DUMP                 = 18;

    /** Packet header sizes */
    public static final int    NET_HEADER_SIZE                 = 4;
    public static final int    SQLSTATE_LENGTH                 = 5;

    /** Packet offsets */
    public static final int    PACKET_LEN_OFFSET               = 0;
    public static final int    PACKET_SEQ_OFFSET               = 3;

    /** Maximum packet length */
    public static final int    MAX_PACKET_LENGTH               = (256 * 256 * 256 - 1);

    /** BINLOG_DUMP options */
    public static final int    BINLOG_DUMP_NON_BLOCK           = 1;
    public static final int    BINLOG_SEND_ANNOTATE_ROWS_EVENT = 2;

    private Connection         conn;
    private OutputStream       mysqlOutput;
    private InputStream        mysqlInput;

    public DirectLogFetcher(){
        super(DEFAULT_INITIAL_CAPACITY, DEFAULT_GROWTH_FACTOR);
    }

    public DirectLogFetcher(final int initialCapacity){
        super(initialCapacity, DEFAULT_GROWTH_FACTOR);
    }

    public DirectLogFetcher(final int initialCapacity, final float growthFactor){
        super(initialCapacity, growthFactor);
    }

    private static final Object unwrapConnection(Object conn, Class<?> connClazz) throws IOException {
        while (!connClazz.isInstance(conn)) {
            try {
                Class<?> connProxy = Class.forName("org.springframework.jdbc.datasource.ConnectionProxy");
                if (connProxy.isInstance(conn)) {
                    conn = invokeMethod(conn, connProxy, "getTargetConnection");
                    continue;
                }
            } catch (ClassNotFoundException e) {
                // org.springframework.jdbc.datasource.ConnectionProxy not
                // found.
            }

            try {
                Class<?> connProxy = Class.forName("org.apache.commons.dbcp.DelegatingConnection");
                if (connProxy.isInstance(conn)) {
                    conn = getDeclaredField(conn, connProxy, "_conn");
                    continue;
                }
            } catch (ClassNotFoundException e) {
                // org.apache.commons.dbcp.DelegatingConnection not found.
            }

            try {
                if (conn instanceof java.sql.Wrapper) {
                    Class<?> connIface = Class.forName("com.mysql.jdbc.Connection");
                    conn = ((java.sql.Wrapper) conn).unwrap(connIface);
                    continue;
                }
            } catch (ClassNotFoundException e) {
                // com.mysql.jdbc.Connection not found.
            } catch (SQLException e) {
                logger.warn("Unwrap " + conn.getClass().getName() + " to " + connClazz.getName() + " failed: "
                            + e.getMessage(),
                    e);
            }

            return null;
        }
        return conn;
    }

    private static final Object invokeMethod(Object obj, Class<?> objClazz, String name) {
        try {
            Method method = objClazz.getMethod(name, (Class<?>[]) null);
            return method.invoke(obj, (Object[]) null);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("No such method: \'" + name + "\' @ " + objClazz.getName(), e);
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException("Cannot invoke method: \'" + name + "\' @ " + objClazz.getName(), e);
        } catch (InvocationTargetException e) {
            throw new IllegalArgumentException("Invoke method failed: \'" + name + "\' @ " + objClazz.getName(),
                e.getTargetException());
        }
    }

    private static final Object getDeclaredField(Object obj, Class<?> objClazz, String name) {
        try {
            Field field = objClazz.getDeclaredField(name);
            field.setAccessible(true);
            return field.get(obj);
        } catch (NoSuchFieldException e) {
            throw new IllegalArgumentException("No such field: \'" + name + "\' @ " + objClazz.getName(), e);
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException("Cannot get field: \'" + name + "\' @ " + objClazz.getName(), e);
        }
    }

    /**
     * Connect MySQL master to fetch binlog.
     */
    public void open(Connection conn, String fileName, final int serverId) throws IOException {
        open(conn, fileName, BIN_LOG_HEADER_SIZE, serverId, false);
    }

    /**
     * Connect MySQL master to fetch binlog.
     */
    public void open(Connection conn, String fileName, final int serverId, boolean nonBlocking) throws IOException {
        open(conn, fileName, BIN_LOG_HEADER_SIZE, serverId, nonBlocking);
    }

    /**
     * Connect MySQL master to fetch binlog.
     */
    public void open(Connection conn, String fileName, final long filePosition, final int serverId) throws IOException {
        open(conn, fileName, filePosition, serverId, false);
    }

    /**
     * Connect MySQL master to fetch binlog.
     */
    public void open(Connection conn, String fileName, long filePosition, final int serverId, boolean nonBlocking)
                                                                                                                  throws IOException {
        try {
            this.conn = conn;
            Class<?> connClazz = Class.forName("com.mysql.jdbc.ConnectionImpl");
            Object unwrapConn = unwrapConnection(conn, connClazz);
            if (unwrapConn == null) {
                throw new IOException("Unable to unwrap " + conn.getClass().getName()
                                      + " to com.mysql.jdbc.ConnectionImpl");
            }

            // Get underlying IO streams for network communications.
            Object connIo = getDeclaredField(unwrapConn, connClazz, "io");
            if (connIo == null) {
                throw new IOException("Get null field:" + conn.getClass().getName() + "#io");
            }
            mysqlOutput = (OutputStream) getDeclaredField(connIo, connIo.getClass(), "mysqlOutput");
            mysqlInput = (InputStream) getDeclaredField(connIo, connIo.getClass(), "mysqlInput");

            if (filePosition == 0) filePosition = BIN_LOG_HEADER_SIZE;
            sendBinlogDump(fileName, filePosition, serverId, nonBlocking);
            position = 0;
        } catch (IOException e) {
            close(); /* Do cleanup */
            logger.error("Error on COM_BINLOG_DUMP: file = " + fileName + ", position = " + filePosition);
            throw e;
        } catch (ClassNotFoundException e) {
            close(); /* Do cleanup */
            throw new IOException("Unable to load com.mysql.jdbc.ConnectionImpl", e);
        }
    }

    /**
     * Put a byte in the buffer.
     * 
     * @param b the byte to put in the buffer
     */
    protected final void putByte(byte b) {
        ensureCapacity(position + 1);

        buffer[position++] = b;
    }

    /**
     * Put 16-bit integer in the buffer.
     * 
     * @param i16 the integer to put in the buffer
     */
    protected final void putInt16(int i16) {
        ensureCapacity(position + 2);

        byte[] buf = buffer;
        buf[position++] = (byte) (i16 & 0xff);
        buf[position++] = (byte) (i16 >>> 8);
    }

    /**
     * Put 32-bit integer in the buffer.
     * 
     * @param i32 the integer to put in the buffer
     */
    protected final void putInt32(long i32) {
        ensureCapacity(position + 4);

        byte[] buf = buffer;
        buf[position++] = (byte) (i32 & 0xff);
        buf[position++] = (byte) (i32 >>> 8);
        buf[position++] = (byte) (i32 >>> 16);
        buf[position++] = (byte) (i32 >>> 24);
    }

    /**
     * Put a string in the buffer.
     * 
     * @param s the value to put in the buffer
     */
    protected final void putString(String s) {
        ensureCapacity(position + (s.length() * 2) + 1);

        System.arraycopy(s.getBytes(), 0, buffer, position, s.length());
        position += s.length();
        buffer[position++] = 0;
    }

    protected final void sendBinlogDump(String fileName, final long filePosition, final int serverId,
                                        boolean nonBlocking) throws IOException {
        position = NET_HEADER_SIZE;

        putByte(COM_BINLOG_DUMP);
        putInt32(filePosition);
        int binlog_flags = nonBlocking ? BINLOG_DUMP_NON_BLOCK : 0;
        binlog_flags |= BINLOG_SEND_ANNOTATE_ROWS_EVENT;
        putInt16(binlog_flags); // binlog_flags
        putInt32(serverId); // slave's server-id
        putString(fileName);

        final byte[] buf = buffer;
        final int len = position - NET_HEADER_SIZE;
        buf[0] = (byte) (len & 0xff);
        buf[1] = (byte) (len >>> 8);
        buf[2] = (byte) (len >>> 16);

        mysqlOutput.write(buffer, 0, position);
        mysqlOutput.flush();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.taobao.tddl.dbsync.binlog.LogFetcher#fetch()
     */
    public boolean fetch() throws IOException {
        try {
            // Fetching packet header from input.
            if (!fetch0(0, NET_HEADER_SIZE)) {
                logger.warn("Reached end of input stream while fetching header");
                return false;
            }

            // Fetching the first packet(may a multi-packet).
            int netlen = getUint24(PACKET_LEN_OFFSET);
            int netnum = getUint8(PACKET_SEQ_OFFSET);
            if (!fetch0(NET_HEADER_SIZE, netlen)) {
                logger.warn("Reached end of input stream: packet #" + netnum + ", len = " + netlen);
                return false;
            }

            // Detecting error code.
            final int mark = getUint8(NET_HEADER_SIZE);
            if (mark != 0) {
                if (mark == 255) // error from master
                {
                    // Indicates an error, for example trying to fetch from
                    // wrong
                    // binlog position.
                    position = NET_HEADER_SIZE + 1;
                    final int errno = getInt16();
                    String sqlstate = forward(1).getFixString(SQLSTATE_LENGTH);
                    String errmsg = getFixString(limit - position);
                    throw new IOException("Received error packet:" + " errno = " + errno + ", sqlstate = " + sqlstate
                                          + " errmsg = " + errmsg);
                } else if (mark == 254) {
                    // Indicates end of stream. It's not clear when this would
                    // be sent.
                    logger.warn("Received EOF packet from server, apparent" + " master disconnected.");
                    return false;
                } else {
                    // Should not happen.
                    throw new IOException("Unexpected response " + mark + " while fetching binlog: packet #" + netnum
                                          + ", len = " + netlen);
                }
            }

            // The first packet is a multi-packet, concatenate the packets.
            while (netlen == MAX_PACKET_LENGTH) {
                if (!fetch0(0, NET_HEADER_SIZE)) {
                    logger.warn("Reached end of input stream while fetching header");
                    return false;
                }

                netlen = getUint24(PACKET_LEN_OFFSET);
                netnum = getUint8(PACKET_SEQ_OFFSET);
                if (!fetch0(limit, netlen)) {
                    logger.warn("Reached end of input stream: packet #" + netnum + ", len = " + netlen);
                    return false;
                }
            }

            // Preparing buffer variables to decoding.
            origin = NET_HEADER_SIZE + 1;
            position = origin;
            limit -= origin;
            return true;
        } catch (SocketTimeoutException e) {
            close(); /* Do cleanup */
            logger.error("Socket timeout expired, closing connection", e);
            throw e;
        } catch (InterruptedIOException e) {
            close(); /* Do cleanup */
            logger.warn("I/O interrupted while reading from client socket", e);
            throw e;
        } catch (IOException e) {
            close(); /* Do cleanup */
            logger.error("I/O error while reading from client socket", e);
            throw e;
        }
    }

    private final boolean fetch0(final int off, final int len) throws IOException {
        ensureCapacity(off + len);

        for (int count, n = 0; n < len; n += count) {
            if (0 > (count = mysqlInput.read(buffer, off + n, len - n))) {
                // Reached end of input stream
                return false;
            }
        }

        if (limit < off + len) limit = off + len;
        return true;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.taobao.tddl.dbsync.binlog.LogFetcher#close()
     */
    public void close() throws IOException {
        try {
            if (conn != null) conn.close();

            conn = null;
            mysqlInput = null;
            mysqlOutput = null;
        } catch (SQLException e) {
            logger.warn("Unable to close connection", e);
        }
    }
}
