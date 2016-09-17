package com.alibaba.otter.canal.parse.driver.mysql.packets;

import java.io.IOException;

/**
 * Top Abstraction for network packet.<br>
 * it exposes 2 behaviors for sub-class implementation which will be used to
 * marshal data into bytes before sending and to un-marshal data from data after
 * receiving.<br>
 * 
 * @author fujohnwang
 * @see 1.0
 */
public interface IPacket {

    /**
     * un-marshal raw bytes into {@link IPacket} state for application usage.<br>
     * 
     * @param data, the raw byte data received from networking
     */
    void fromBytes(byte[] data) throws IOException;

    /**
     * marshal the {@link IPacket} state into raw bytes for sending out to
     * network.<br>
     * 
     * @return the bytes that's collected from {@link IPacket} state
     */
    byte[] toBytes() throws IOException;
}
