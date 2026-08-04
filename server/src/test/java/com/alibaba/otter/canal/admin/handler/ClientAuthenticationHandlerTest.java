package com.alibaba.otter.canal.admin.handler;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelSink;
import org.jboss.netty.channel.MessageEvent;
import org.junit.Assert;
import org.junit.Test;

import com.alibaba.otter.canal.admin.CanalAdmin;
import com.alibaba.otter.canal.protocol.AdminPacket;
import com.alibaba.otter.canal.protocol.AdminPacket.Ack;
import com.alibaba.otter.canal.protocol.AdminPacket.Packet;
import com.google.protobuf.ByteString;

/**
 * Unit tests for {@link ClientAuthenticationHandler} to ensure authentication
 * failures do not fall-through to the success path (auth bypass).
 */
public class ClientAuthenticationHandlerTest {

    @Test
    public void testAuthFailedShouldCloseChannelAndNotRemoveHandlers() throws Exception {
        // Given a handler with a seed set and an admin that always rejects auth
        ClientAuthenticationHandler handler = new ClientAuthenticationHandler(new RejectingCanalAdmin());
        handler.setSeed(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 });

        MockChannel channel = new MockChannel();
        MockChannelPipeline pipeline = new MockChannelPipeline(channel);
        channel.pipeline = pipeline;
        MockChannelHandlerContext ctx = new MockChannelHandlerContext(channel, pipeline);

        MessageEvent event = buildClientAuthEvent("admin", "wrong-password");

        // When the auth packet arrives
        handler.messageReceived(ctx, event);

        // Then only one error packet is written, channel is closed and handlers are NOT removed
        Assert.assertEquals("Expected exactly one write for the error packet", 1, channel.writes.size());
        byte[] written = channel.writes.get(0);
        Packet response = Packet.parseFrom(written);
        Assert.assertEquals(AdminPacket.PacketType.ACK, response.getType());
        Ack ack = Ack.parseFrom(response.getBody());
        Assert.assertTrue("Expected error ack", ack.getCode() != 0);
        Assert.assertTrue("Channel should be closed on auth failure", channel.closed);
        Assert.assertEquals("Auth handler must not be removed on failure", 0, pipeline.removedHandlers.size());
    }

    @Test
    public void testNullSeedShouldCloseChannelAndNotRemoveHandlers() throws Exception {
        // Given a handler with NO seed set (e.g. handshake not completed)
        ClientAuthenticationHandler handler = new ClientAuthenticationHandler(new AcceptingCanalAdmin());
        // seed intentionally left null

        MockChannel channel = new MockChannel();
        MockChannelPipeline pipeline = new MockChannelPipeline(channel);
        channel.pipeline = pipeline;
        MockChannelHandlerContext ctx = new MockChannelHandlerContext(channel, pipeline);

        MessageEvent event = buildClientAuthEvent("admin", "any-password");

        handler.messageReceived(ctx, event);

        Assert.assertEquals("Expected exactly one write for the error packet", 1, channel.writes.size());
        Packet response = Packet.parseFrom(channel.writes.get(0));
        Assert.assertEquals(AdminPacket.PacketType.ACK, response.getType());
        Ack ack = Ack.parseFrom(response.getBody());
        Assert.assertTrue("Expected error ack", ack.getCode() != 0);
        Assert.assertTrue("Channel should be closed when seed is null", channel.closed);
        Assert.assertEquals("Auth handler must not be removed on failure", 0, pipeline.removedHandlers.size());
    }

    @Test
    public void testAuthSuccessShouldWriteAckAndRemoveHandlers() throws Exception {
        // Given a handler with a seed set and an admin that accepts auth
        ClientAuthenticationHandler handler = new ClientAuthenticationHandler(new AcceptingCanalAdmin());
        handler.setSeed(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 });

        MockChannel channel = new MockChannel();
        MockChannelPipeline pipeline = new MockChannelPipeline(channel);
        channel.pipeline = pipeline;
        MockChannelHandlerContext ctx = new MockChannelHandlerContext(channel, pipeline);

        MessageEvent event = buildClientAuthEvent("admin", "correct-password");

        handler.messageReceived(ctx, event);

        Assert.assertEquals("Expected exactly one write for the success ack", 1, channel.writes.size());
        Packet response = Packet.parseFrom(channel.writes.get(0));
        Assert.assertEquals(AdminPacket.PacketType.ACK, response.getType());
        Ack ack = Ack.parseFrom(response.getBody());
        Assert.assertEquals("Expected success ack", 0, ack.getCode());
        Assert.assertFalse("Channel should NOT be closed on auth success", channel.closed);
        Assert.assertEquals("Both handshake and auth handlers should be removed", 2, pipeline.removedHandlers.size());
    }

    private MessageEvent buildClientAuthEvent(String username, String password) {
        AdminPacket.ClientAuth clientAuth = AdminPacket.ClientAuth.newBuilder()
            .setUsername(username)
            .setPassword(ByteString.copyFromUtf8(password))
            .build();

        Packet packet = Packet.newBuilder()
            .setType(AdminPacket.PacketType.CLIENTAUTHENTICATION)
            .setVersion(3)
            .setBody(clientAuth.toByteString())
            .build();

        byte[] bytes = packet.toByteArray();
        ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(bytes);
        return new MockMessageEvent(buffer);
    }

    // ------------------------------------------------------------------
    // Simple stub implementations for Netty 3 interfaces used by the tests
    // ------------------------------------------------------------------

    private static class AcceptingCanalAdmin implements CanalAdmin {
        @Override
        public boolean auth(String username, String password, byte[] seed) {
            return true;
        }

        @Override
        public boolean check() {
            return true;
        }

        @Override
        public boolean start() {
            return true;
        }

        @Override
        public boolean stop() {
            return true;
        }

        @Override
        public boolean restart() {
            return true;
        }

        @Override
        public String getRunningInstances() {
            return "";
        }

        @Override
        public boolean checkInstance(String destination) {
            return true;
        }

        @Override
        public boolean startInstance(String destination) {
            return true;
        }

        @Override
        public boolean stopInstance(String destination) {
            return true;
        }

        @Override
        public boolean releaseInstance(String destination) {
            return true;
        }

        @Override
        public boolean restartInstance(String destination) {
            return true;
        }

        @Override
        public String listCanalLog() {
            return "";
        }

        @Override
        public String canalLog(int lines) {
            return "";
        }

        @Override
        public String listInstanceLog(String destination) {
            return "";
        }

        @Override
        public String instanceLog(String destination, String fileName, int lines) {
            return "";
        }
    }

    private static class RejectingCanalAdmin implements CanalAdmin {
        @Override
        public boolean auth(String username, String password, byte[] seed) {
            return false;
        }

        @Override
        public boolean check() {
            return true;
        }

        @Override
        public boolean start() {
            return true;
        }

        @Override
        public boolean stop() {
            return true;
        }

        @Override
        public boolean restart() {
            return true;
        }

        @Override
        public String getRunningInstances() {
            return "";
        }

        @Override
        public boolean checkInstance(String destination) {
            return true;
        }

        @Override
        public boolean startInstance(String destination) {
            return true;
        }

        @Override
        public boolean stopInstance(String destination) {
            return true;
        }

        @Override
        public boolean releaseInstance(String destination) {
            return true;
        }

        @Override
        public boolean restartInstance(String destination) {
            return true;
        }

        @Override
        public String listCanalLog() {
            return "";
        }

        @Override
        public String canalLog(int lines) {
            return "";
        }

        @Override
        public String listInstanceLog(String destination) {
            return "";
        }

        @Override
        public String instanceLog(String destination, String fileName, int lines) {
            return "";
        }
    }

    private static class MockChannelFuture implements ChannelFuture {

        @Override
        public Channel getChannel() {
            return null;
        }

        @Override
        public boolean isDone() {
            return true;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isSuccess() {
            return true;
        }

        @Override
        public Throwable getCause() {
            return null;
        }

        @Override
        public boolean cancel() {
            return false;
        }

        @Override
        public boolean setSuccess() {
            return true;
        }

        @Override
        public boolean setFailure(Throwable cause) {
            return false;
        }

        @Override
        public boolean setProgress(long amount, long current, long total) {
            return false;
        }

        @Override
        public void addListener(ChannelFutureListener listener) {
            try {
                listener.operationComplete(this);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void removeListener(ChannelFutureListener listener) {
        }

        @Override
        public ChannelFuture await() throws InterruptedException {
            return this;
        }

        @Override
        public ChannelFuture awaitUninterruptibly() {
            return this;
        }

        @Override
        public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
            return true;
        }

        @Override
        public boolean await(long timeoutMillis) throws InterruptedException {
            return true;
        }

        @Override
        public boolean awaitUninterruptibly(long timeout, TimeUnit unit) {
            return true;
        }

        @Override
        public boolean awaitUninterruptibly(long timeoutMillis) {
            return true;
        }
    }

    private static class MockChannel implements Channel {

        final List<byte[]> writes = new ArrayList<>();
        boolean              closed = false;
        MockChannelPipeline  pipeline;

        @Override
        public ChannelFuture write(Object message) {
            byte[] data = ((ChannelBuffer) message).array();
            writes.add(data);
            return new MockChannelFuture();
        }

        @Override
        public ChannelFuture write(Object message, SocketAddress remoteAddress) {
            return write(message);
        }

        @Override
        public ChannelFuture close() {
            closed = true;
            return new MockChannelFuture();
        }

        @Override
        public ChannelPipeline getPipeline() {
            return pipeline;
        }

        @Override
        public Integer getId() {
            return 0;
        }

        @Override
        public ChannelFactory getFactory() {
            return null;
        }

        @Override
        public Channel getParent() {
            return null;
        }

        @Override
        public org.jboss.netty.channel.ChannelConfig getConfig() {
            return null;
        }

        @Override
        public boolean isOpen() {
            return !closed;
        }

        @Override
        public boolean isBound() {
            return false;
        }

        @Override
        public boolean isConnected() {
            return false;
        }

        @Override
        public SocketAddress getLocalAddress() {
            return null;
        }

        @Override
        public SocketAddress getRemoteAddress() {
            return null;
        }

        @Override
        public ChannelFuture bind(SocketAddress localAddress) {
            return null;
        }

        @Override
        public ChannelFuture connect(SocketAddress remoteAddress) {
            return null;
        }

        @Override
        public ChannelFuture disconnect() {
            return null;
        }

        @Override
        public ChannelFuture unbind() {
            return null;
        }

        @Override
        public ChannelFuture getCloseFuture() {
            return null;
        }

        @Override
        public int getInterestOps() {
            return 0;
        }

        @Override
        public boolean isReadable() {
            return false;
        }

        @Override
        public boolean isWritable() {
            return false;
        }

        @Override
        public ChannelFuture setInterestOps(int interestOps) {
            return null;
        }

        @Override
        public ChannelFuture setReadable(boolean readable) {
            return null;
        }

        @Override
        public int compareTo(Channel o) {
            return 0;
        }
    }

    private static class MockChannelPipeline implements ChannelPipeline {

        final MockChannel              channel;
        final List<String>             removedHandlers = new ArrayList<>();

        MockChannelPipeline(MockChannel channel) {
            this.channel = channel;
        }

        @Override
        public Channel getChannel() {
            return channel;
        }

        @Override
        public void sendDownstream(ChannelEvent e) {
            if (e instanceof MessageEvent) {
                Object msg = ((MessageEvent) e).getMessage();
                if (msg instanceof ChannelBuffer) {
                    ChannelBuffer buf = (ChannelBuffer) msg;
                    byte[] data = new byte[buf.readableBytes()];
                    buf.getBytes(buf.readerIndex(), data);
                    // AdminNettyUtils prepends a 4-byte length header; strip it
                    // before storing the protobuf body for easier assertions.
                    byte[] body = new byte[data.length - 4];
                    System.arraycopy(data, 4, body, 0, body.length);
                    channel.writes.add(body);
                }
            }
            e.getFuture().setSuccess();
        }

        @Override
        public void remove(ChannelHandler handler) {
            removedHandlers.add(handler.getClass().getName());
        }

        @Override
        public ChannelHandler remove(String name) {
            removedHandlers.add(name);
            return null;
        }

        @Override
        public <T extends ChannelHandler> T remove(Class<T> handlerType) {
            removedHandlers.add(handlerType.getName());
            return null;
        }

        // Unused methods
        @Override
        public void addFirst(String name, ChannelHandler handler) {
        }

        @Override
        public void addLast(String name, ChannelHandler handler) {
        }

        @Override
        public void addBefore(String baseName, String name, ChannelHandler handler) {
        }

        @Override
        public void addAfter(String baseName, String name, ChannelHandler handler) {
        }

        @Override
        public ChannelHandler removeFirst() {
            return null;
        }

        @Override
        public ChannelHandler removeLast() {
            return null;
        }

        @Override
        public void replace(ChannelHandler oldHandler, String newName, ChannelHandler newHandler) {
        }

        @Override
        public ChannelHandler replace(String oldName, String newName, ChannelHandler newHandler) {
            return null;
        }

        @Override
        public <T extends ChannelHandler> T replace(Class<T> oldHandlerType, String newName, ChannelHandler newHandler) {
            return null;
        }

        @Override
        public ChannelHandler getFirst() {
            return null;
        }

        @Override
        public ChannelHandler getLast() {
            return null;
        }

        @Override
        public ChannelHandler get(String name) {
            return null;
        }

        @Override
        public <T extends ChannelHandler> T get(Class<T> handlerType) {
            return null;
        }

        @Override
        public ChannelHandlerContext getContext(ChannelHandler handler) {
            return null;
        }

        @Override
        public ChannelHandlerContext getContext(String name) {
            return null;
        }

        @Override
        public ChannelHandlerContext getContext(Class<? extends ChannelHandler> handlerType) {
            return null;
        }

        @Override
        public void sendUpstream(ChannelEvent e) {
        }

        @Override
        public ChannelSink getSink() {
            return null;
        }

        @Override
        public void attach(Channel channel, ChannelSink sink) {
        }

        @Override
        public boolean isAttached() {
            return false;
        }

        @Override
        public List<String> getNames() {
            return null;
        }

        @Override
        public Map<String, ChannelHandler> toMap() {
            return null;
        }
    }

    private static class MockChannelHandlerContext implements ChannelHandlerContext {

        private final Channel         channel;
        private final ChannelPipeline pipeline;

        MockChannelHandlerContext(Channel channel, ChannelPipeline pipeline) {
            this.channel = channel;
            this.pipeline = pipeline;
        }

        @Override
        public Channel getChannel() {
            return channel;
        }

        @Override
        public ChannelPipeline getPipeline() {
            return pipeline;
        }

        @Override
        public String getName() {
            return null;
        }

        @Override
        public ChannelHandler getHandler() {
            return null;
        }

        @Override
        public boolean canHandleUpstream() {
            return false;
        }

        @Override
        public boolean canHandleDownstream() {
            return false;
        }

        @Override
        public void sendUpstream(ChannelEvent e) {
        }

        @Override
        public void sendDownstream(ChannelEvent e) {
        }

        @Override
        public Object getAttachment() {
            return null;
        }

        @Override
        public void setAttachment(Object attachment) {
        }
    }

    private static class MockMessageEvent implements MessageEvent {

        private final ChannelBuffer message;

        MockMessageEvent(ChannelBuffer message) {
            this.message = message;
        }

        @Override
        public Object getMessage() {
            return message;
        }

        @Override
        public SocketAddress getRemoteAddress() {
            return null;
        }

        @Override
        public Channel getChannel() {
            return null;
        }

        @Override
        public ChannelFuture getFuture() {
            return null;
        }
    }
}
