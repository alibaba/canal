package com.alibaba.otter.canal.parse.driver.mysql.socket;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;

/**
 * 使用BIO进行dump
 *
 * @author chuanyi
 */
public class SocketChannel {

    static final int         DEFAULT_CONNECT_TIMEOUT = 10 * 1000;
    static final int         SO_TIMEOUT              = 1000;
    private Socket           socket;
    private InputStream      input;
    private OutputStream     output;

    SocketChannel(Socket socket) throws IOException {
        this.socket = socket;
        this.input = socket.getInputStream();
        this.output = socket.getOutputStream();
    }

    public void writeChannel(byte[]... buf) throws IOException {
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
                    throw new InterruptedIOException("Interrupted while reading.");
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
                    throw new InterruptedIOException("Interrupted while reading.");
                }
                accTimeout += SO_TIMEOUT;
            }
        }
        if (remain > 0 && accTimeout >= timeout) {
            throw new SocketTimeoutException("Timeout occurred, failed to read " + readSize + " bytes in " + timeout + " milliseconds.");
        }
        return data;
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

    public void close() {
        Socket socket = this.socket;
        if (socket != null) {
            try {
                socket.shutdownInput();
            } catch (IOException e) {
                //Ignore, could not do anymore
            }
            try {
                socket.shutdownOutput();
            } catch (IOException e) {
                //Ignore, could not do anymore
            }
            try {
                socket.close();
            } catch (IOException e) {
                //Ignore, could not do anymore
            }
        }
        this.input = null;
        this.output = null;
        this.socket = null;
    }

}
