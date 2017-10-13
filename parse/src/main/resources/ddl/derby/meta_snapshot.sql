CREATE TABLE meta_snapshot (
  id bigint GENERATED ALWAYS AS IDENTITY NOT NULL,
  gmt_create timestamp NOT NULL,
  gmt_modified timestamp NOT NULL,
  destination varchar(128) DEFAULT NULL,
  binlog_file varchar(64) DEFAULT NULL,
  binlog_offest bigint DEFAULT NULL,
  binlog_master_id varchar(64) DEFAULT NULL,
  binlog_timestamp bigint DEFAULT NULL,
  data clob(16 M) DEFAULT NULL,
  extra varchar(512) DEFAULT NULL,
  PRIMARY KEY (id),
  CONSTRAINT meta_snapshot_binlog_file_offest UNIQUE (destination,binlog_master_id,binlog_file,binlog_offest)
);

create index meta_snapshot_destination on meta_snapshot(destination);
create index meta_snapshot_destination_timestamp on meta_snapshot(destination,binlog_timestamp);
create index meta_snapshot_gmt_modified on meta_snapshot(gmt_modified);