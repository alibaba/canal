CREATE TABLE `tb_wbrbzvpfra` (
  `col_lokvnorvsp` char(235) CHARACTER SET utf8mb4 DEFAULT NULL,
  `col_vazlbmjjtl` char(1) DEFAULT NULL,
  `col_kcxzselokz` mediumint(229) unsigned NOT NULL DEFAULT '1',
  `col_bjiezlcrgf` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') NOT NULL DEFAULT 'enum_or_set_0',
  PRIMARY KEY (`col_kcxzselokz`,`col_bjiezlcrgf`),
  UNIQUE KEY `uk_qxibrmyivk` (`col_bjiezlcrgf`),
  UNIQUE KEY `uk_zmxfbsrjpe` (`col_bjiezlcrgf`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
