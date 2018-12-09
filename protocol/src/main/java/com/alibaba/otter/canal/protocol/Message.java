package com.alibaba.otter.canal.protocol;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.alibaba.otter.canal.common.utils.CanalToStringStyle;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.google.protobuf.ByteString;

/**
 * @author zebin.xuzb @ 2012-6-19
 * @version 1.0.0
 */
public class Message implements Serializable {

    private static final long      serialVersionUID = 1234034768477580009L;

    private long                   id;
    private List<CanalEntry.Entry> entries          = new ArrayList<CanalEntry.Entry>();
    // row data for performance, see:
    // https://github.com/alibaba/canal/issues/726
    private boolean                raw              = true;
    private List<ByteString>       rawEntries       = new ArrayList<ByteString>();

    public Message(long id, List<Entry> entries){
        this.id = id;
        this.entries = entries == null ? new ArrayList<Entry>() : entries;
        this.raw = false;
    }

    public Message(long id, boolean raw, List entries){
        this.id = id;
        if (raw) {
            this.rawEntries = entries == null ? new ArrayList<ByteString>() : entries;
        } else {
            this.entries = entries == null ? new ArrayList<Entry>() : entries;
        }
        this.raw = raw;
    }

    public Message(long id){
        this.id = id;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public List<Entry> getEntries() {
        return entries;
    }

    public void setEntries(List<CanalEntry.Entry> entries) {
        this.entries = entries;
    }

    public void addEntry(CanalEntry.Entry entry) {
        this.entries.add(entry);
    }

    public void setRawEntries(List<ByteString> rawEntries) {
        this.rawEntries = rawEntries;
    }

    public void addRawEntry(ByteString rawEntry) {
        this.rawEntries.add(rawEntry);
    }

    public List<ByteString> getRawEntries() {
        return rawEntries;
    }

    public boolean isRaw() {
        return raw;
    }

    public void setRaw(boolean raw) {
        this.raw = raw;
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, CanalToStringStyle.DEFAULT_STYLE);
    }

    /**
     * 将 message 分区
     * 
     * @param partitionsNum 分区数
     * @param pkHashConfigs 分区库表主键正则表达式
     * @return 分区message数组
     */
    @SuppressWarnings("unchecked")
    public Message[] messagePartition(Integer partitionsNum, String pkHashConfigs) {
        if (partitionsNum == null) {
            partitionsNum = 1;
        }
        Message[] partitionMessages = new Message[partitionsNum];
        List<Entry>[] partitionEntries = new List[partitionsNum];
        for (int i = 0; i < partitionsNum; i++) {
            partitionEntries[i] = new ArrayList<>();
        }
        for (Entry entry : this.getEntries()) {
            CanalEntry.RowChange rowChange;
            try {
                rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
            if (rowChange.getIsDdl()) {
                partitionEntries[0].add(entry);
            } else {
                if (rowChange.getRowDatasList() != null && !rowChange.getRowDatasList().isEmpty()) {
                    String database = entry.getHeader().getSchemaName();
                    String table = entry.getHeader().getTableName();

                    String pk = null;
                    boolean isMatch = false;

                    String[] pkHashConfigArray = StringUtils.split(pkHashConfigs, ",");
                    for (String pkHashConfig : pkHashConfigArray) {
                        int i = pkHashConfig.lastIndexOf(".");
                        if (!pkHashConfig.endsWith(".$pk$")) {
                            // 如果指定了主键
                            pk = pkHashConfig.substring(i + 1);
                        }
                        pkHashConfig = pkHashConfig.substring(0, i);
                        isMatch = Pattern.matches(pkHashConfig, database + "." + table);
                        if (isMatch) {
                            break;
                        }
                    }

                    if (!isMatch) {
                        // 如果都没有匹配，发送到第一个分区
                        partitionEntries[0].add(entry);
                    } else {
                        if (pk == null) {
                            // 如果未指定主键(通配符主键)，取主键字段
                            try {
                                rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                            } catch (Exception e) {
                                throw new RuntimeException(e.getMessage(), e);
                            }
                            CanalEntry.RowData rowData = rowChange.getRowDatasList().get(0);
                            for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                                if (column.getIsKey()) {
                                    pk = column.getName();
                                    break;
                                }
                            }
                        }
                    }
                    if (pk == null) {
                        // 如果都没有匹配的主键，发送到第一个分区
                        partitionEntries[0].add(entry);
                    } else {
                        for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                            boolean hasOldPk = false;
                            // 如果before中有pk值说明主键有修改, 以旧的主键值hash为准
                            for (CanalEntry.Column column : rowData.getBeforeColumnsList()) {
                                if (column.getName().equalsIgnoreCase(pk)) {
                                    int hash = column.getValue().hashCode();
                                    int pkHash = Math.abs(hash) % partitionsNum;
                                    pkHash = Math.abs(pkHash);
                                    partitionEntries[pkHash].add(entry);
                                    hasOldPk = true;
                                    break;
                                }
                            }
                            if (!hasOldPk) {
                                for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                                    if (column.getName().equalsIgnoreCase(pk)) {
                                        int hash = column.getValue().hashCode();
                                        int pkHash = Math.abs(hash) % partitionsNum;
                                        pkHash = Math.abs(pkHash);
                                        partitionEntries[pkHash].add(entry);
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        for (int i = 0; i < partitionsNum; i++) {
            List<Entry> entries = partitionEntries[i];
            if (!entries.isEmpty()) {
                partitionMessages[i] = new Message(this.id, entries);
            }
        }

        return partitionMessages;
    }
}
