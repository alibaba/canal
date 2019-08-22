CREATE TABLE `tb_hxrlnujcrv` (
  `col_fkstgdmdmv` varbinary(215) DEFAULT '\0',
  `col_oyzofwwoch` mediumblob,
  `col_exhsgobpvc` tinyint(246) zerofill NOT NULL,
  `col_ypcuorizvb` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8 NULL DEFAULT 'enum_or_set_0',
  PRIMARY KEY (`col_exhsgobpvc`),
  CONSTRAINT UNIQUE `col_exhsgobpvc` (`col_exhsgobpvc`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `tb_njohawksae` (
  `col_knuawjkxqc` year DEFAULT '2019',
  `col_bhfbpfgzco` mediumtext CHARACTER SET utf8,
  `col_aqkzimqpda` bit(37) DEFAULT b'0',
  CONSTRAINT UNIQUE `uk_enknmzojsn` (`col_knuawjkxqc`),
  UNIQUE KEY `col_aqkzimqpda` (`col_aqkzimqpda`)
) DEFAULT CHARSET=utf8;
CREATE TABLE `tb_iztgdcpcld` (
  `col_rvnhrlexjo` time DEFAULT '00:00:00',
  `col_fwwgntdsdt` smallint zerofill,
  `col_yxejjdacht` int(213) zerofill NOT NULL,
  UNIQUE KEY `uk_svckkuvqce` (`col_rvnhrlexjo`,`col_fwwgntdsdt`),
  UNIQUE INDEX `uk_amgxauyvbp` (`col_rvnhrlexjo`,`col_fwwgntdsdt`)
) DEFAULT CHARSET=latin1;
CREATE TABLE `tb_lsynbmkqdy` LIKE `tb_hxrlnujcrv`;
CREATE TABLE `tb_lqihveibyp` (
  `col_ttqqizkwig` bit(43) NULL DEFAULT b'0',
  `col_yvgpfjhjcj` timestamp(5) NULL,
  `col_cuvrdliegh` mediumint unsigned zerofill
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE TABLE `tb_vkycuerrcd` (
  `col_ennnhbifuc` tinytext CHARACTER SET utf8,
  `col_argkmjdhlo` text(1982098738) CHARACTER SET utf8,
  `col_nkqouukkqn` date DEFAULT '2019-07-04',
  CONSTRAINT `symb_rkcyrcasyq` UNIQUE `uk_dmsqvpcazv` (`col_nkqouukkqn`),
  UNIQUE KEY `uk_stwkfixzhd` (`col_ennnhbifuc`(11),`col_nkqouukkqn`)
) DEFAULT CHARSET=utf8;
RENAME TABLE `tb_njohawksae` TO `tb_gcvlfgdqvw`;
RENAME TABLE `tb_lqihveibyp` TO `tb_sugaoprlwz`;
RENAME TABLE `tb_hxrlnujcrv` TO `tb_qqtidoozva`;
RENAME TABLE `tb_sugaoprlwz` TO `tb_kubnsybcaf`;
RENAME TABLE `tb_lsynbmkqdy` TO `tb_ssvyhzkjrr`;
RENAME TABLE `tb_vkycuerrcd` TO `tb_skbxuhpnhx`, `tb_gcvlfgdqvw` TO `tb_evibhvcuqj`;
RENAME TABLE `tb_skbxuhpnhx` TO `tb_qlelzwesdb`, `tb_qqtidoozva` TO `tb_bjsplgjemd`;
RENAME TABLE `tb_ssvyhzkjrr` TO `tb_dxadqwfhhb`, `tb_evibhvcuqj` TO `tb_zflebwrowg`;
DROP TABLE tb_zflebwrowg;
DROP TABLE tb_qlelzwesdb;
CREATE TABLE `tb_hxrlnujcrv` (
  `col_fkstgdmdmv` varbinary(215) DEFAULT '\0',
  `col_oyzofwwoch` mediumblob,
  `col_exhsgobpvc` tinyint(246) zerofill NOT NULL,
  `col_ypcuorizvb` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8 NULL DEFAULT 'enum_or_set_0',
  PRIMARY KEY (`col_exhsgobpvc`),
  CONSTRAINT UNIQUE `col_exhsgobpvc` (`col_exhsgobpvc`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `tb_njohawksae` (
  `col_knuawjkxqc` year DEFAULT '2019',
  `col_bhfbpfgzco` mediumtext CHARACTER SET utf8,
  `col_aqkzimqpda` bit(37) DEFAULT b'0',
  CONSTRAINT UNIQUE `uk_enknmzojsn` (`col_knuawjkxqc`),
  UNIQUE KEY `col_aqkzimqpda` (`col_aqkzimqpda`)
) DEFAULT CHARSET=utf8;
CREATE TABLE `tb_iztgdcpcld` (
  `col_rvnhrlexjo` time DEFAULT '00:00:00',
  `col_fwwgntdsdt` smallint zerofill,
  `col_yxejjdacht` int(213) zerofill NOT NULL,
  UNIQUE KEY `uk_svckkuvqce` (`col_rvnhrlexjo`,`col_fwwgntdsdt`),
  UNIQUE INDEX `uk_amgxauyvbp` (`col_rvnhrlexjo`,`col_fwwgntdsdt`)
) DEFAULT CHARSET=latin1;
CREATE TABLE `tb_lsynbmkqdy` LIKE `tb_hxrlnujcrv`;
CREATE TABLE `tb_lqihveibyp` (
  `col_ttqqizkwig` bit(43) NULL DEFAULT b'0',
  `col_yvgpfjhjcj` timestamp(5) NULL,
  `col_cuvrdliegh` mediumint unsigned zerofill
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE TABLE `tb_vkycuerrcd` (
  `col_ennnhbifuc` tinytext CHARACTER SET utf8,
  `col_argkmjdhlo` text(1982098738) CHARACTER SET utf8,
  `col_nkqouukkqn` date DEFAULT '2019-07-04',
  CONSTRAINT `symb_rkcyrcasyq` UNIQUE `uk_dmsqvpcazv` (`col_nkqouukkqn`),
  UNIQUE KEY `uk_stwkfixzhd` (`col_ennnhbifuc`(11),`col_nkqouukkqn`)
) DEFAULT CHARSET=utf8;
RENAME TABLE `tb_njohawksae` TO `tb_gcvlfgdqvw`;
RENAME TABLE `tb_lqihveibyp` TO `tb_sugaoprlwz`;
RENAME TABLE `tb_hxrlnujcrv` TO `tb_qqtidoozva`;
RENAME TABLE `tb_sugaoprlwz` TO `tb_kubnsybcaf`;
RENAME TABLE `tb_lsynbmkqdy` TO `tb_ssvyhzkjrr`;
RENAME TABLE `tb_vkycuerrcd` TO `tb_skbxuhpnhx`, `tb_gcvlfgdqvw` TO `tb_evibhvcuqj`;
RENAME TABLE `tb_skbxuhpnhx` TO `tb_qlelzwesdb`, `tb_qqtidoozva` TO `tb_bjsplgjemd`;
RENAME TABLE `tb_ssvyhzkjrr` TO `tb_dxadqwfhhb`, `tb_evibhvcuqj` TO `tb_zflebwrowg`;
DROP TABLE tb_zflebwrowg;
DROP TABLE tb_qlelzwesdb;
CREATE TABLE `tb_hxrlnujcrv` (
  `col_fkstgdmdmv` varbinary(215) DEFAULT '\0',
  `col_oyzofwwoch` mediumblob,
  `col_exhsgobpvc` tinyint(246) zerofill NOT NULL,
  `col_ypcuorizvb` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8 NULL DEFAULT 'enum_or_set_0',
  PRIMARY KEY (`col_exhsgobpvc`),
  CONSTRAINT UNIQUE `col_exhsgobpvc` (`col_exhsgobpvc`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `tb_njohawksae` (
  `col_knuawjkxqc` year DEFAULT '2019',
  `col_bhfbpfgzco` mediumtext CHARACTER SET utf8,
  `col_aqkzimqpda` bit(37) DEFAULT b'0',
  CONSTRAINT UNIQUE `uk_enknmzojsn` (`col_knuawjkxqc`),
  UNIQUE KEY `col_aqkzimqpda` (`col_aqkzimqpda`)
) DEFAULT CHARSET=utf8;
CREATE TABLE `tb_iztgdcpcld` (
  `col_rvnhrlexjo` time DEFAULT '00:00:00',
  `col_fwwgntdsdt` smallint zerofill,
  `col_yxejjdacht` int(213) zerofill NOT NULL,
  UNIQUE KEY `uk_svckkuvqce` (`col_rvnhrlexjo`,`col_fwwgntdsdt`),
  UNIQUE INDEX `uk_amgxauyvbp` (`col_rvnhrlexjo`,`col_fwwgntdsdt`)
) DEFAULT CHARSET=latin1;
CREATE TABLE `tb_lsynbmkqdy` LIKE `tb_hxrlnujcrv`;
CREATE TABLE `tb_lqihveibyp` (
  `col_ttqqizkwig` bit(43) NULL DEFAULT b'0',
  `col_yvgpfjhjcj` timestamp(5) NULL,
  `col_cuvrdliegh` mediumint unsigned zerofill
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE TABLE `tb_vkycuerrcd` (
  `col_ennnhbifuc` tinytext CHARACTER SET utf8,
  `col_argkmjdhlo` text(1982098738) CHARACTER SET utf8,
  `col_nkqouukkqn` date DEFAULT '2019-07-04',
  CONSTRAINT `symb_rkcyrcasyq` UNIQUE `uk_dmsqvpcazv` (`col_nkqouukkqn`),
  UNIQUE KEY `uk_stwkfixzhd` (`col_ennnhbifuc`(11),`col_nkqouukkqn`)
) DEFAULT CHARSET=utf8;
RENAME TABLE `tb_njohawksae` TO `tb_gcvlfgdqvw`;
RENAME TABLE `tb_lqihveibyp` TO `tb_sugaoprlwz`;
RENAME TABLE `tb_hxrlnujcrv` TO `tb_qqtidoozva`;
RENAME TABLE `tb_sugaoprlwz` TO `tb_kubnsybcaf`;
RENAME TABLE `tb_lsynbmkqdy` TO `tb_ssvyhzkjrr`;
RENAME TABLE `tb_vkycuerrcd` TO `tb_skbxuhpnhx`, `tb_gcvlfgdqvw` TO `tb_evibhvcuqj`;
RENAME TABLE `tb_skbxuhpnhx` TO `tb_qlelzwesdb`, `tb_qqtidoozva` TO `tb_bjsplgjemd`;
RENAME TABLE `tb_ssvyhzkjrr` TO `tb_dxadqwfhhb`, `tb_evibhvcuqj` TO `tb_zflebwrowg`;
DROP TABLE tb_zflebwrowg;
DROP TABLE tb_qlelzwesdb;
CREATE TABLE `tb_hxrlnujcrv` (
  `col_fkstgdmdmv` varbinary(215) DEFAULT '\0',
  `col_oyzofwwoch` mediumblob,
  `col_exhsgobpvc` tinyint(246) zerofill NOT NULL,
  `col_ypcuorizvb` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8 NULL DEFAULT 'enum_or_set_0',
  PRIMARY KEY (`col_exhsgobpvc`),
  CONSTRAINT UNIQUE `col_exhsgobpvc` (`col_exhsgobpvc`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `tb_njohawksae` (
  `col_knuawjkxqc` year DEFAULT '2019',
  `col_bhfbpfgzco` mediumtext CHARACTER SET utf8,
  `col_aqkzimqpda` bit(37) DEFAULT b'0',
  CONSTRAINT UNIQUE `uk_enknmzojsn` (`col_knuawjkxqc`),
  UNIQUE KEY `col_aqkzimqpda` (`col_aqkzimqpda`)
) DEFAULT CHARSET=utf8;
CREATE TABLE `tb_iztgdcpcld` (
  `col_rvnhrlexjo` time DEFAULT '00:00:00',
  `col_fwwgntdsdt` smallint zerofill,
  `col_yxejjdacht` int(213) zerofill NOT NULL,
  UNIQUE KEY `uk_svckkuvqce` (`col_rvnhrlexjo`,`col_fwwgntdsdt`),
  UNIQUE INDEX `uk_amgxauyvbp` (`col_rvnhrlexjo`,`col_fwwgntdsdt`)
) DEFAULT CHARSET=latin1;
CREATE TABLE `tb_lsynbmkqdy` LIKE `tb_hxrlnujcrv`;
CREATE TABLE `tb_lqihveibyp` (
  `col_ttqqizkwig` bit(43) NULL DEFAULT b'0',
  `col_yvgpfjhjcj` timestamp(5) NULL,
  `col_cuvrdliegh` mediumint unsigned zerofill
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE TABLE `tb_vkycuerrcd` (
  `col_ennnhbifuc` tinytext CHARACTER SET utf8,
  `col_argkmjdhlo` text(1982098738) CHARACTER SET utf8,
  `col_nkqouukkqn` date DEFAULT '2019-07-04',
  CONSTRAINT `symb_rkcyrcasyq` UNIQUE `uk_dmsqvpcazv` (`col_nkqouukkqn`),
  UNIQUE KEY `uk_stwkfixzhd` (`col_ennnhbifuc`(11),`col_nkqouukkqn`)
) DEFAULT CHARSET=utf8;
RENAME TABLE `tb_njohawksae` TO `tb_gcvlfgdqvw`;
RENAME TABLE `tb_lqihveibyp` TO `tb_sugaoprlwz`;
RENAME TABLE `tb_hxrlnujcrv` TO `tb_qqtidoozva`;
RENAME TABLE `tb_sugaoprlwz` TO `tb_kubnsybcaf`;
RENAME TABLE `tb_lsynbmkqdy` TO `tb_ssvyhzkjrr`;
RENAME TABLE `tb_vkycuerrcd` TO `tb_skbxuhpnhx`, `tb_gcvlfgdqvw` TO `tb_evibhvcuqj`;
RENAME TABLE `tb_skbxuhpnhx` TO `tb_qlelzwesdb`, `tb_qqtidoozva` TO `tb_bjsplgjemd`;
RENAME TABLE `tb_ssvyhzkjrr` TO `tb_dxadqwfhhb`, `tb_evibhvcuqj` TO `tb_zflebwrowg`;
DROP TABLE tb_zflebwrowg;
DROP TABLE tb_qlelzwesdb;
