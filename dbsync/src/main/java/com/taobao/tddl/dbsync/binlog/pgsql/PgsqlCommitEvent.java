package com.taobao.tddl.dbsync.binlog.pgsql;

import com.taobao.tddl.dbsync.binlog.event.LogHeader;

public class PgsqlCommitEvent extends PgsqlLogEvent {

  public PgsqlCommitEvent(LogHeader header) {
    super(header);
  }

}
