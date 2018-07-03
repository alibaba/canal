package com.alibaba.otter.canal.parse.driver.mysql.socket;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author luoyaogui 实现channel的管理（监听连接、读数据、回收） 2016-12-28
 */
@SuppressWarnings({ "rawtypes", "deprecation" })
public abstract class NettySocketChannelPool {

    private static EventLoopGroup              group     = new NioEventLoopGroup();                              // 非阻塞IO线程组
    private static Bootstrap                   boot      = new Bootstrap();                                      // 主
    private static Map<Channel, SocketChannel> chManager = new ConcurrentHashMap<Channel, SocketChannel>();
    private static final Logger                logger    = LoggerFactory.getLogger(NettySocketChannelPool.class);

    static {
        boot.group(group)
            .channel(NioSocketChannel.class)
            .option(ChannelOption.TCP_NODELAY, true)
            // 如果是延时敏感型应用，建议关闭Nagle算法
            .option(ChannelOption.SO_KEEPALIVE, true)
            .option(ChannelOption.SO_REUSEADDR, true)
            .option(ChannelOption.RCVBUF_ALLOCATOR, AdaptiveRecvByteBufAllocator.DEFAULT)
            .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
            //
            .handler(new ChannelInitializer() {

                @Override
                protected void initChannel(Channel ch) throws Exception {
                    ch.pipeline().addLast(new BusinessHandler());// 命令过滤和handler添加管理
                }
            });
    }

    public static SocketChannel open(SocketAddress address) throws Exception {
        SocketChannel socket = null;
        ChannelFuture future = boot.connect(address).sync();

        if (future.isSuccess()) {
            future.channel().pipeline().get(BusinessHandler.class).latch.await();
            socket = chManager.get(future.channel());
        }

        if (null == socket) {
            throw new IOException("can't create socket!");
        }

        return socket;
    }

    public static class BusinessHandler extends SimpleChannelInboundHandler<ByteBuf> {

        private NettySocketChannel   socket = null;
        private final CountDownLatch latch  = new CountDownLatch(1);

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            socket.setChannel(null);
            chManager.remove(ctx.channel());// 移除
            super.channelInactive(ctx);
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            socket = new NettySocketChannel();
            socket.setChannel(ctx.channel());
            chManager.put(ctx.channel(), socket);
            latch.countDown();
            super.channelActive(ctx);
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
            if (socket != null) {
                socket.writeCache(msg);
            } else {
                // TODO: need graceful error handler.
                logger.error("no socket available.");
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            // need output error for troubeshooting.
            logger.error("business error.", cause);
            ctx.close();
        }
    }
}
