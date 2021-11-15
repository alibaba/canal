package com.alibaba.otter.canal.parse.inbound.pgsql;

import com.alibaba.otter.canal.filter.aviater.AviaterRegexFilter;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.inbound.AbstractBinlogParser;
import com.alibaba.otter.canal.parse.inbound.TableMeta;
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.LogEventConvert;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.Header;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.Type;
import com.google.protobuf.ByteString;
import com.taobao.tddl.dbsync.binlog.pgsql.PgsqlBeginEvent;
import com.taobao.tddl.dbsync.binlog.pgsql.PgsqlCommitEvent;
import com.taobao.tddl.dbsync.binlog.pgsql.PgsqlLogEvent;
import com.taobao.tddl.dbsync.binlog.pgsql.PgsqlRowEvent;
import java.sql.Timestamp;
import org.apache.commons.lang.StringUtils;

public class PgsqlBinlogParser extends AbstractBinlogParser<PgsqlLogEvent> {

  private PgsqlMetaCache tableMetaCache;
  private AviaterRegexFilter nameFilter;

  public PgsqlBinlogParser() {
  }

  @Override
  public Entry parse(PgsqlLogEvent event, boolean isSeek) throws CanalParseException {
    if (event instanceof PgsqlBeginEvent) {
      return parseBegin((PgsqlBeginEvent) event);
    } else if (event instanceof PgsqlCommitEvent) {
      return parseCommit((PgsqlCommitEvent) event);
    } else if (event instanceof PgsqlRowEvent) {
      return parseRow((PgsqlRowEvent) event);
    }
    throw new CanalParseException("unknown event:" + event);
  }

  @Override
  public Entry parse(PgsqlLogEvent event, TableMeta tableMeta) throws CanalParseException {
    return parse(event, false);
  }

  @Override
  public Entry parse(PgsqlLogEvent event) throws CanalParseException {
    return parse(event, false);
  }

  @Override
  public void reset() {
    if (tableMetaCache != null) {
      tableMetaCache.clear();
    }
  }

  protected Entry parseBegin(PgsqlBeginEvent rd) {
    String data = rd.getData();
    String xid = data.substring(data.indexOf(' ') + 1);
    return Entry.newBuilder()
        .setHeader(Header.newBuilder()
            .setSchemaName("")
            .setTableName("")
            // 下面几个在记录position时需要
            .setLogfileName(rd.getDestination())
            .setLogfileOffset(rd.getLsn())
            .setExecuteTime(System.currentTimeMillis()) // begin的binlog没有时间,等到后面转换时在替换
            .setServerId(rd.getServerId())
            .setGtid(xid + "_" + rd.getLsn())
            .setSourceType(Type.PGSQL)
            .build())
        .setEntryType(EntryType.TRANSACTIONBEGIN)
        .build();
  }

  protected Entry parseCommit(PgsqlCommitEvent rd) {
    // commit xid (at yyyy-MM-dd HH:mm:ss.SSSSSS+00)
    String data = rd.getData();
    int i = data.indexOf(' ');
    int j = data.indexOf(" (at");
    String xid = data.substring(i + 1, j);
    String time = data.substring(j + 5, data.length() - 4);// 去掉时区信息

    Timestamp timestamp = Timestamp.valueOf(time);
    long executeTime = timestamp.getTime() - (timestamp.getNanos() / 1000000);

    return Entry.newBuilder()
        .setHeader(Header.newBuilder()
            .setSchemaName("")
            .setTableName("")
            .setLogfileName(rd.getDestination())
            .setLogfileOffset(rd.getLsn())
            .setExecuteTime(executeTime)
            .setServerId(rd.getServerId())
            .setGtid(xid + "_" + rd.getLsn())
            .setSourceType(Type.PGSQL)
            .build())
        .setEntryType(EntryType.TRANSACTIONEND)
        .build();
  }

  protected Entry parseRow(PgsqlRowEvent rd) {

    String row = rd.getData();
    // row = table drc.tb1: INSERT: id[integer]:1 name[character varying]:'tom1' age[integer]:11 update_time[timestamp without time zone]:'2021-04-14 11:08:21.508507'

    String schema;
    String table;
    String type;

    int idx = row.indexOf(' ');

    // row = drc.tb1: INSERT: id[integer]:1 name[character varying]:'tom1' age[integer]:11 update_time[timestamp without time zone]:'2021-04-14 11:08:21.508507'
    row = row.substring(idx + 1);
    idx = row.indexOf('.');
    schema = row.substring(0, idx);

    // row = tb1: INSERT: id[integer]:1 name[character varying]:'tom1' age[integer]:11 update_time[timestamp without time zone]:'2021-04-14 11:08:21.508507'
    row = row.substring(idx + 1);
    idx = row.indexOf(':');
    table = row.substring(0, idx);

    TableMeta tableMeta = getTableMeta(schema, table);

    // row = INSERT: id[integer]:1 name[character varying]:'tom1' age[integer]:11 update_time[timestamp without time zone]:'2021-04-14 11:08:21.508507'
    row = row.substring(idx + 2);

    idx = row.indexOf(':');
    type = row.substring(0, idx);
    CanalEntry.EventType eventType = CanalEntry.EventType.valueOf(type);

    // row = id[integer]:1 name[character varying]:'tom1' age[integer]:11 update_time[timestamp without time zone]:'2021-04-14 11:08:21.508507'
    row = row.substring(idx + 2);
    if (row.contains("(no-tuple-data)")) {
      // delete时无主键
      return null;
    }

    if (this.nameFilter != null && !this.nameFilter.filter(schema + "." + table)) {
      return null;
    }

    int index = 0;
    CanalEntry.RowData.Builder rowDataBuilder = CanalEntry.RowData.newBuilder();
    do {
      row = parseOneColumn(rowDataBuilder, index++, row, tableMeta);
    } while (row.length() > 0);

    CanalEntry.RowData rowData = rowDataBuilder.build();

    Header header = Header
        .newBuilder()
        .setSchemaName(schema)
        .setTableName(table)
        .setLogfileName(rd.getDestination())
        .setLogfileOffset(rd.getEventLen())
        .setServerId(rd.getServerId())
        .setGtid("")
        .setSourceType(Type.PGSQL)
        .setEventType(eventType)
        .setExecuteTime(System.currentTimeMillis())
        .build();

    ByteString rawValue = RowChange.newBuilder().addRowDatas(rowData)
        .setEventType(eventType)
        .setTableId(0L)
        .setIsDdl(false)
        .build()
        .toByteString();
    return LogEventConvert.createEntry(
        header,
        EntryType.ROWDATA,
        rawValue);
  }

  protected String parseOneColumn(CanalEntry.RowData.Builder rowDataBuilder, int index, String row, TableMeta tbMeta) {
    // name
    int idx = row.indexOf('[');
    String colName = row.substring(0, idx);
    String colType;
    String colData;

    boolean isOldPk;// 更改了主键
    boolean isPk;

    if (colName.contains("old-key:")) {
      int i = colName.indexOf(": ");
      colName = colName.substring(i + 2);
      isOldPk = true;
      isPk = false;
    } else {
      if (colName.contains("new-tuple:")) {
        int i = colName.indexOf(": ");
        colName = colName.substring(i + 2);
        isPk = true;
      } else {
        colName = colName.trim();
        String n = colName;

        isPk = tbMeta.getPrimaryFields().stream()
            .anyMatch(fm -> fm.isKey() && StringUtils.equalsIgnoreCase(fm.getColumnName(), n));
      }
      isOldPk = false;
    }

    // type
    row = row.substring(idx + 1);
    idx = row.indexOf("]:");
    colType = row.substring(0, idx);
    row = row.substring(idx + 2);

    // data
    StringBuilder d = new StringBuilder();
    boolean isNull = false;
    int i = 0;

    if (row.charAt(i) == '\'') {
      i++;
      while (i < row.length()) {
        if (row.charAt(i) == '\'' && i + 1 < row.length() && row.charAt(i + 1) == '\'') {
          d.append("'");
          i += 1;
        } else if (row.charAt(i) == '\'') {
          break;
        } else {
          d.append(row.charAt(i));
        }
        i++;
      }
      colData = d.toString();
    } else {
      while (i < row.length()) {
        if (row.charAt(i) == ' ') {
          break;
        } else if (i == row.length() - 1) {
          d.append(row.charAt(i));
          i++;
          break;
        } else {
          d.append(row.charAt(i));
        }
        i++;
      }
      colData = d.toString();
      isNull = "null".equals(colData);
    }
    if (row.length() > i) {
      row = row.substring(i + 1);
    } else {
      row = row.substring(i);
    }

    // 注意: 大字段问题,一般只有在更新时才会出现,此时该字段没有发生变化,直接跳过该字段
    if ("unchanged-toast-datum".equals(colData)) {
      return row;
    }

    CanalEntry.Column.Builder columnBuilder = CanalEntry.Column.newBuilder();
    columnBuilder.setIsKey(isPk);
    columnBuilder.setMysqlType(colType);
    columnBuilder.setIndex(index);
    columnBuilder.setIsNull(isNull);
    columnBuilder.setValue(colData);
    columnBuilder.setSqlType(-1);
    columnBuilder.setUpdated(true);
    columnBuilder.setName(colName);

    if (isOldPk) {
      rowDataBuilder.addBeforeColumns(columnBuilder.build());
    } else {
      rowDataBuilder.addAfterColumns(columnBuilder.build());
    }
    return row;
  }

  protected TableMeta getTableMeta(String schema, String table) {
    return this.tableMetaCache.getTableMeta(schema, table);
  }

  // region getter/setter

  public PgsqlMetaCache getTableMetaCache() {
    return tableMetaCache;
  }

  public void setTableMetaCache(PgsqlMetaCache tableMetaCache) {
    this.tableMetaCache = tableMetaCache;
  }

  public AviaterRegexFilter getNameFilter() {
    return nameFilter;
  }

  public void setNameFilter(AviaterRegexFilter nameFilter) {
    this.nameFilter = nameFilter;
  }

  // endregion getter/setter
}
