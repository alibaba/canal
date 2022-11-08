package com.alibaba.otter.canal.parse.driver.mysql.socket;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedByInterruptException;

/**
 * 使用BIO进行dump
 *
 * @author chuanyi
 */
public class BioSocketChannel implements SocketChannel {

    static final int     DEFAULT_CONNECT_TIMEOUT = 10 * 1000;
    static final int     SO_TIMEOUT              = 1000;
    private Socket       socket;
    private InputStream  input;
    private OutputStream output;

    BioSocketChannel(Socket socket) throws IOException{
        this.socket = socket;
        this.input = new BufferedInputStream(socket.getInputStream(), 16384);
        this.output = socket.getOutputStream();
    }

    public void write(byte[]... buf) throws IOException {
        OutputStream output = this.output;
        if (output != null) {
            for (byte[] bs : buf) {
                output.write(bs);
            }
        } else {
            throw new SocketException("Socket already closed.");
        }
    }

    public byte[] read(int readSize) throws IOException {
        InputStream input = this.input;
        byte[] data = new byte[readSize];
        int remain = readSize;
        if (input == null) {
            throw new SocketException("Socket already closed.");
        }
        while (remain > 0) {
            try {
                int read = input.read(data, readSize - remain, remain);
                if (read > -1) {
                    remain -= read;
                } else {
                    throw new IOException("EOF encountered.");
                }
            } catch (SocketTimeoutException te) {
                if (Thread.interrupted()) {
                    throw new ClosedByInterruptException();
                }
            }
        }
        return data;
    }

    public byte[] read(int readSize, int timeout) throws IOException {
        InputStream input = this.input;
        byte[] data = new byte[readSize];
        int remain = readSize;
        int accTimeout = 0;
        if (input == null) {
            throw new SocketException("Socket already closed.");
        }
        while (remain > 0 && accTimeout < timeout) {
            try {
                int read = input.read(data, readSize - remain, remain);
                if (read > -1) {
                    remain -= read;
                } else {
                    throw new IOException("EOF encountered.");
                }
            } catch (SocketTimeoutException te) {
                if (Thread.interrupted()) {
                    throw new ClosedByInterruptException();
                }
                accTimeout += SO_TIMEOUT;
            }
        }
        if (remain > 0 && accTimeout >= timeout) {
            throw new SocketTimeoutException("Timeout occurred, failed to read total " + readSize + " bytes in "
                                             + timeout + " milliseconds, actual read only " + (readSize - remain)
                                             + " bytes");
        }
        return data;
    }

    @Override
    public void read(byte[] data, int off, int len, int timeout) throws IOException {
        InputStream input = this.input;
        int accTimeout = 0;
        if (input == null) {
            throw new SocketException("Socket already closed.");
        }

        int n = 0;
        while (n < len && accTimeout < timeout) {
            try {
                int read = input.read(data, off + n, len - n);
                if (read > -1) {
                    n += read;
                } else {
                    throw new IOException("EOF encountered.");
                }
            } catch (SocketTimeoutException te) {
                if (Thread.interrupted()) {
                    throw new ClosedByInterruptException();
                }
                accTimeout += SO_TIMEOUT;
            }
        }

        if (n < len && accTimeout >= timeout) {
            throw new SocketTimeoutException("Timeout occurred, failed to read total " + len + " bytes in " + timeout
                                             + " milliseconds, actual read only " + n + " bytes");
        }
    }

    public boolean isConnected() {
        Socket socket = this.socket;
        if (socket != null) {
            return socket.isConnected();
        }
        return false;
    }

    public SocketAddress getRemoteSocketAddress() {
        Socket socket = this.socket;
        if (socket != null) {
            return socket.getRemoteSocketAddress();
        }

        return null;
    }

    public SocketAddress getLocalSocketAddress() {
        Socket socket = this.socket;
        if (socket != null) {
            return socket.getLocalSocketAddress();
        }

        return null;
    }

    public void close() {
        Socket socket = this.socket;
        if (socket != null) {
            try {
                socket.shutdownInput();
            } catch (IOException e) {
                // Ignore, could not do anymore
            }
            try {
                socket.shutdownOutput();
            } catch (IOException e) {
                // Ignore, could not do anymore
            }
            try {
                socket.close();
            } catch (IOException e) {
                // Ignore, could not do anymore
            }
        }
        this.input = null;
        this.output = null;
        this.socket = null;
    }

}
