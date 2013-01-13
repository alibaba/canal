package com.alibaba.otter.canal.parse.inbound.emulator.oracle;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import com.alibaba.otter.canal.parse.inbound.emulator.oracle.data.BinLogFile;

/**
 * Oracle erosa server emulator
 * 
 * @author: yuanzu Date: 12-9-24 Time: 下午2:49
 */
public class OracleErosaServer {

    private final int          port;
    private final BinLogFile[] files;
    private Channel            serverChannel = null;
    private ServerBootstrap    bootstrap     = null;

    public OracleErosaServer(int port, BinLogFile[] files){
        this.port = port;
        this.files = files;
    }

    public void run() {
        // Configure the server.
        this.bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(Executors.newCachedThreadPool(),
                                                                               Executors.newCachedThreadPool()));

        // Set up the event pipeline factory.
        bootstrap.setPipelineFactory(new OracleErosaServerPipelineFactory(this.files));

        // Bind and start to accept incoming connections.
        this.serverChannel = bootstrap.bind(new InetSocketAddress(this.port));
    }

    public void terminate() {
        if (this.serverChannel != null) {
            this.serverChannel.close().awaitUninterruptibly(1000);
        }

        if (this.bootstrap != null) {
            this.bootstrap.releaseExternalResources();
        }
    }

    public static void main(String[] args) throws Exception {
        int port;
        BinLogFile[] files = null;

        if (args.length > 0) {
            port = Integer.parseInt(args[0]);
        } else {
            port = 8080;
        }

        if (args.length > 1) {
            files = loadBinLogFiles(args[1]);
        }

        new OracleErosaServer(port, files).run();
    }

    private static BinLogFile[] loadBinLogFiles(String arg) {
        return null;
    }
}
