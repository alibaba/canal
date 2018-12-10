package com.alibaba.otter.canal.protocol;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.alibaba.otter.canal.protocol.aviater.AviaterRegexFilter;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.alibaba.otter.canal.common.utils.CanalToStringStyle;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * @author zebin.xuzb @ 2012-6-19
 * @version 1.0.0
 */
public class Message implements Serializable {

    private static final long                                    serialVersionUID = 1234034768477580009L;

    private static ConcurrentMap<String, String>                 schemaTabPk      = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, AviaterRegexFilter> regexFilters     = new ConcurrentHashMap<>();

    private long                                                 id;
    private List<CanalEntry.Entry>                               entries          = new ArrayList<CanalEntry.Entry>();
    // row data for performance, see:
    // https://github.com/alibaba/canal/issues/726
    private boolean                                              raw              = true;
    private List<ByteString>                                     rawEntries       = new ArrayList<ByteString>();

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

        List<CanalEntry.Entry> entries;
        if (this.isRaw()) {
            List<ByteString> rawEntries = this.getRawEntries();
            entries = new ArrayList<>(rawEntries.size());
            for (ByteString byteString : rawEntries) {
                Entry entry;
                try {
                    entry = Entry.parseFrom(byteString);
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                }
                entries.add(entry);
            }
        } else {
            entries = this.getEntries();
        }

        for (Entry entry : entries) {
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

                        AviaterRegexFilter aviaterRegexFilter = regexFilters.get(pkHashConfig);
                        if (aviaterRegexFilter == null) {
                            aviaterRegexFilter = new AviaterRegexFilter(pkHashConfig);
                            regexFilters.putIfAbsent(pkHashConfig, aviaterRegexFilter);
                        }

                        isMatch = aviaterRegexFilter.filter(database + "." + table);
                        if (isMatch) {
                            break;
                        }
                    }

                    if (!isMatch) {
                        // 如果都没有匹配，发送到第一个分区
                        partitionEntries[0].add(entry);
                    } else {
                        if (pk == null) {
                            pk = schemaTabPk.get(database + "." + table);
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
                                        schemaTabPk.putIfAbsent(database + "." + table, pk);
                                        break;
                                    }
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
            List<Entry> entriesTmp = partitionEntries[i];
            if (!entriesTmp.isEmpty()) {
                partitionMessages[i] = new Message(this.id, entriesTmp);
            }
        }

        return partitionMessages;
    }
}
