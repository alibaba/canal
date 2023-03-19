package com.taobao.tddl.dbsync.binlog.pgsql;

import com.taobao.tddl.dbsync.binlog.event.LogHeader;

public class PgsqlBeginEvent extends PgsqlLogEvent {

  public PgsqlBeginEvent(LogHeader header) {
    super(header);
  }

}
