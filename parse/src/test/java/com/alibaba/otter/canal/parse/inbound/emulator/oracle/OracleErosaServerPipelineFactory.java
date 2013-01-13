package com.alibaba.otter.canal.parse.inbound.emulator.oracle;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;

import com.alibaba.otter.canal.parse.inbound.emulator.oracle.E3PacketDecoder.E3PacketDecoder;
import com.alibaba.otter.canal.parse.inbound.emulator.oracle.data.BinLogFile;

/**
 * oracle erosa server pipeline factory
 * 
 * @author: yuanzu Date: 12-9-24 Time: 下午2:57
 */
public class OracleErosaServerPipelineFactory implements ChannelPipelineFactory {

    private final BinLogFile[] files;

    public OracleErosaServerPipelineFactory(BinLogFile[] files){
        this.files = files;
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline pipeline = Channels.pipeline();

        pipeline.addLast("e3packet", new E3PacketDecoder());

        pipeline.addLast("handler", new OracleErosaServerHandler(this.files));

        return pipeline;
    }
}
