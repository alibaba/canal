CREATE TABLE `tb_bjsplgjemd` (
  `col_fkstgdmdmv` varbinary(215) DEFAULT '\0',
  `col_oyzofwwoch` mediumblob,
  `col_exhsgobpvc` tinyint(246) unsigned zerofill NOT NULL,
  `col_ypcuorizvb` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') DEFAULT 'enum_or_set_0',
  PRIMARY KEY (`col_exhsgobpvc`),
  UNIQUE KEY `col_exhsgobpvc` (`col_exhsgobpvc`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `tb_dxadqwfhhb` (
  `col_fkstgdmdmv` varbinary(215) DEFAULT '\0',
  `col_oyzofwwoch` mediumblob,
  `col_exhsgobpvc` tinyint(246) unsigned zerofill NOT NULL,
  `col_ypcuorizvb` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') DEFAULT 'enum_or_set_0',
  PRIMARY KEY (`col_exhsgobpvc`),
  UNIQUE KEY `col_exhsgobpvc` (`col_exhsgobpvc`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `tb_iztgdcpcld` (
  `col_rvnhrlexjo` time DEFAULT '00:00:00',
  `col_fwwgntdsdt` smallint(5) unsigned zerofill DEFAULT NULL,
  `col_yxejjdacht` int(213) unsigned zerofill NOT NULL,
  UNIQUE KEY `uk_svckkuvqce` (`col_rvnhrlexjo`,`col_fwwgntdsdt`),
  UNIQUE KEY `uk_amgxauyvbp` (`col_rvnhrlexjo`,`col_fwwgntdsdt`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE TABLE `tb_kubnsybcaf` (
  `col_ttqqizkwig` bit(43) DEFAULT b'0',
  `col_yvgpfjhjcj` timestamp(5) NULL DEFAULT NULL,
  `col_cuvrdliegh` mediumint(8) unsigned zerofill DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
