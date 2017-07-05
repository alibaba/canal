package com.alibaba.otter.canal.parse.driver.mysql.socket;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.ReferenceCountUtil;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author luoyaogui 实现channel的管理（监听连接、读数据、回收） 2016-12-28
 */
@SuppressWarnings({ "rawtypes", "deprecation" })
public abstract class SocketChannelPool {

    private static EventLoopGroup              group     = new NioEventLoopGroup();                         // 非阻塞IO线程组
    private static Bootstrap                   boot      = new Bootstrap();                                 // 主
    private static Map<Channel, SocketChannel> chManager = new ConcurrentHashMap<Channel, SocketChannel>();

    static {
        boot.group(group)
            .channel(NioSocketChannel.class)
            .option(ChannelOption.SO_RCVBUF, 32 * 1024)
            .option(ChannelOption.SO_SNDBUF, 32 * 1024)
            .option(ChannelOption.TCP_NODELAY, true)
            // 如果是延时敏感型应用，建议关闭Nagle算法
            .option(ChannelOption.SO_KEEPALIVE, true)
            .option(ChannelOption.RCVBUF_ALLOCATOR, AdaptiveRecvByteBufAllocator.DEFAULT)
            .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
            //
            .handler(new ChannelInitializer() {

                @Override
                protected void initChannel(Channel arg0) throws Exception {
                    arg0.pipeline().addLast(new BusinessHandler());// 命令过滤和handler添加管理
                }
            });
    }

    public static SocketChannel open(SocketAddress address) throws Exception {
        final SocketChannel socket = new SocketChannel();
        boot.connect(address).addListener(new ChannelFutureListener() {

            @Override
            public void operationComplete(ChannelFuture arg0) throws Exception {
                if (arg0.isSuccess()) socket.setChannel(arg0.channel());
                synchronized (socket) {
                    socket.notify();
                }
            }
        });
        synchronized (socket) {
            socket.wait();
        }
        if (null == socket.getChannel()) {
            throw new IOException("can't create socket!");
        }
        chManager.put(socket.getChannel(), socket);
        return socket;
    }

    public static class BusinessHandler extends ChannelInboundHandlerAdapter {

        private SocketChannel socket = null;

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            socket.setChannel(null);
            chManager.remove(ctx.channel());// 移除
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (null == socket) socket = chManager.get(ctx.channel());
            if (socket != null) {
                socket.writeCache((ByteBuf) msg);
            }
            ReferenceCountUtil.release(msg);// 添加防止内存泄漏的
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            ctx.close();
        }
    }
}
