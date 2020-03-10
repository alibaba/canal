CREATE TABLE procs_priv (
Host char(60) COLLATE utf8_bin NOT NULL DEFAULT '',
Db char(64) COLLATE utf8_bin NOT NULL DEFAULT '',
User char(16) COLLATE utf8_bin NOT NULL DEFAULT '',
Routine_name char(64) CHARACTER SET utf8 NOT NULL DEFAULT '',
Routine_type enum('FUNCTION','PROCEDURE') COLLATE utf8_bin NOT NULL,
Grantor char(77) COLLATE utf8_bin NOT NULL DEFAULT '',
Proc_priv set('Execute','Alter Routine','Grant') CHARACTER SET utf8 NOT NULL DEFAULT '',
Timestamp timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
PRIMARY KEY (Host,Db,User,Routine_name,Routine_type),
KEY Grantor (Grantor)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='Procedure privileges'