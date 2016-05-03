package com.alibaba.otter.canal.parse.inbound.mysql;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;

import com.alibaba.otter.canal.parse.CanalEventParser;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.inbound.ErosaConnection;
import com.alibaba.otter.canal.parse.index.CanalLogPositionManager;
import com.alibaba.otter.canal.parse.support.AuthenticationInfo;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.alibaba.otter.canal.protocol.position.LogPosition;

/**
 * 基于本地binlog文件的复制
 * 
 * @author jianghang 2012-6-21 下午04:07:33
 * @version 1.0.0
 */
public class LocalBinlogEventParser extends AbstractMysqlEventParser implements CanalEventParser {

    // 数据库信息
    private AuthenticationInfo masterInfo;
    private EntryPosition      masterPosition;        // binlog信息

    private String             directory;
    private boolean            needWait   = false;
    private int                bufferSize = 16 * 1024;

    public LocalBinlogEventParser(){
        // this.runningInfo = new AuthenticationInfo();
    }

    @Override
    protected ErosaConnection buildErosaConnection() {
        return buildLocalBinLogConnection();
    }

    public void start() throws CanalParseException {
        if (runningInfo == null) { // 第一次链接主库
            runningInfo = masterInfo;
        }

        super.start();
    }

    private ErosaConnection buildLocalBinLogConnection() {
        LocalBinLogConnection connection = new LocalBinLogConnection();

        connection.setBufferSize(this.bufferSize);
        connection.setDirectory(this.directory);
        connection.setNeedWait(this.needWait);

        return connection;
    }
    
    @Override
	protected EntryPosition findEndPosition(ErosaConnection connection) throws IOException {
		// TODO 没有实现
		return findStartPosition(connection);
	}

    @Override
    protected EntryPosition findStartPosition(ErosaConnection connection) {
        // 处理逻辑
        // 1. 首先查询上一次解析成功的最后一条记录
        // 2. 存在最后一条记录，判断一下当前记录是否发生过主备切换
        // // a. 无机器切换，直接返回
        // // b. 存在机器切换，按最后一条记录的stamptime进行查找
        // 3. 不存在最后一条记录，则从默认的位置开始启动
        LogPosition logPosition = logPositionManager.getLatestIndexBy(destination);
        if (logPosition == null) {// 找不到历史成功记录
            EntryPosition entryPosition = masterPosition;

            // 判断一下是否需要按时间订阅
            if (StringUtils.isEmpty(entryPosition.getJournalName())) {
                // 如果没有指定binlogName，尝试按照timestamp进行查找
                if (entryPosition.getTimestamp() != null) {
                    return new EntryPosition(entryPosition.getTimestamp());
                }
            } else {
                if (entryPosition.getPosition() != null) {
                    // 如果指定binlogName + offest，直接返回
                    return entryPosition;
                } else {
                    return new EntryPosition(entryPosition.getTimestamp());
                }
            }
        } else {
            return logPosition.getPostion();
        }

        return null;
    }

    // ========================= setter / getter =========================

    public void setLogPositionManager(CanalLogPositionManager logPositionManager) {
        this.logPositionManager = logPositionManager;
    }

    public void setDirectory(String directory) {
        this.directory = directory;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public void setMasterPosition(EntryPosition masterPosition) {
        this.masterPosition = masterPosition;
    }

    public void setMasterInfo(AuthenticationInfo masterInfo) {
        this.masterInfo = masterInfo;
    }

    public boolean isNeedWait() {
        return needWait;
    }

    public void setNeedWait(boolean needWait) {
        this.needWait = needWait;
    }
}
