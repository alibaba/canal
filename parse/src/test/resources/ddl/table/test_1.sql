CREATE TABLE `tb_quilrfojmp` (
  `col_ruygkecjzp` longblob,
  `col_orycjbfrss` year DEFAULT '2019',
  `col_aqqnunnega` varbinary(225),
  `col_pxwauaruqw` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8 DEFAULT 'enum_or_set_0',
  UNIQUE `col_ruygkecjzp` (`col_ruygkecjzp`(14),`col_orycjbfrss`)
) DEFAULT CHARSET=utf8;
CREATE TABLE `tb_rpuyybsdob` (
  `col_qmvlxemsmt` datetime(6),
  `col_wcntsrfsjf` int NULL DEFAULT '1',
  `col_kekvofvtus` integer(70) unsigned DEFAULT '1',
  `col_bhbfokpfhm` float
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE TABLE `tb_qfmtewribz` (
  `col_hocmjkikme` int unsigned,
  `col_qbggcmwxyh` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8mb4 NOT NULL,
  `col_fulwqoqveo` mediumint unsigned zerofill,
  `col_eniifaecwj` integer(159) NOT NULL DEFAULT '1',
  PRIMARY KEY (`col_qbggcmwxyh`,`col_eniifaecwj`),
  UNIQUE INDEX `uk_fmvsmeqnst` (`col_fulwqoqveo`),
  CONSTRAINT `symb_xifnljgzcv` UNIQUE (`col_qbggcmwxyh`,`col_fulwqoqveo`)
) DEFAULT CHARSET=latin1;
CREATE TABLE `tb_odnffrvnvl` LIKE `tb_quilrfojmp`;
CREATE TABLE `tb_lttgkqunzl` (
  `col_nllztggwyd` smallint unsigned DEFAULT '1'
) DEFAULT CHARSET=utf8;
CREATE TABLE `tb_rkvcdukyuq` LIKE `tb_quilrfojmp`;
CREATE TABLE `tb_erkppdannn` (
  `col_zmjbwpsrnw` timestamp(3) NULL DEFAULT CURRENT_TIMESTAMP(3),
  `col_qqzmxlmztj` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8mb4 NULL DEFAULT 'enum_or_set_0',
  `col_hwnlilelpf` smallint unsigned DEFAULT '1',
  `col_oyeoemafea` date NULL DEFAULT '2019-07-04',
  CONSTRAINT `symb_biynehpjlu` UNIQUE (`col_hwnlilelpf`,`col_oyeoemafea`),
  CONSTRAINT `symb_yrommseaif` UNIQUE `uk_ztxuhpyaxo` (`col_zmjbwpsrnw`,`col_oyeoemafea`)
) DEFAULT CHARSET=utf8;
CREATE TABLE `tb_knsgjmsufx` LIKE `tb_odnffrvnvl`;
CREATE TABLE `tb_ljqeyzesru` LIKE `tb_rpuyybsdob`;
CREATE TABLE `tb_szlgabmztl` (
  `col_hfydymybbg` tinytext CHARACTER SET latin1,
  CONSTRAINT `symb_gpchhhvhwm` UNIQUE KEY (`col_hfydymybbg`(20))
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
RENAME TABLE `tb_qfmtewribz` TO `tb_qeokuaneue`;
RENAME TABLE `tb_lttgkqunzl` TO `tb_uhjgsigdfr`, `tb_erkppdannn` TO `tb_xeoclzosgr`;
RENAME TABLE `tb_quilrfojmp` TO `tb_hycsuwyoxr`, `tb_knsgjmsufx` TO `tb_bbvvzzczcw`;
RENAME TABLE `tb_xeoclzosgr` TO `tb_fxxptceefr`;
RENAME TABLE `tb_odnffrvnvl` TO `tb_rtnozmledi`;
RENAME TABLE `tb_fxxptceefr` TO `tb_tekgceqrmv`;
RENAME TABLE `tb_hycsuwyoxr` TO `tb_hcvvpyqbtd`, `tb_szlgabmztl` TO `tb_yfjonexaag`;
RENAME TABLE `tb_tekgceqrmv` TO `tb_wewsxxqmxd`, `tb_uhjgsigdfr` TO `tb_mbvuditpbx`;
DROP TABLE tb_mbvuditpbx, tb_rkvcdukyuq;
DROP TABLE tb_rtnozmledi, tb_qeokuaneue;
CREATE TABLE `tb_quilrfojmp` (
  `col_ruygkecjzp` longblob,
  `col_orycjbfrss` year DEFAULT '2019',
  `col_aqqnunnega` varbinary(225),
  `col_pxwauaruqw` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8 DEFAULT 'enum_or_set_0',
  UNIQUE `col_ruygkecjzp` (`col_ruygkecjzp`(14),`col_orycjbfrss`)
) DEFAULT CHARSET=utf8;
CREATE TABLE `tb_rpuyybsdob` (
  `col_qmvlxemsmt` datetime(6),
  `col_wcntsrfsjf` int NULL DEFAULT '1',
  `col_kekvofvtus` integer(70) unsigned DEFAULT '1',
  `col_bhbfokpfhm` float
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE TABLE `tb_qfmtewribz` (
  `col_hocmjkikme` int unsigned,
  `col_qbggcmwxyh` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8mb4 NOT NULL,
  `col_fulwqoqveo` mediumint unsigned zerofill,
  `col_eniifaecwj` integer(159) NOT NULL DEFAULT '1',
  PRIMARY KEY (`col_qbggcmwxyh`,`col_eniifaecwj`),
  UNIQUE INDEX `uk_fmvsmeqnst` (`col_fulwqoqveo`),
  CONSTRAINT `symb_xifnljgzcv` UNIQUE (`col_qbggcmwxyh`,`col_fulwqoqveo`)
) DEFAULT CHARSET=latin1;
CREATE TABLE `tb_odnffrvnvl` LIKE `tb_quilrfojmp`;
CREATE TABLE `tb_lttgkqunzl` (
  `col_nllztggwyd` smallint unsigned DEFAULT '1'
) DEFAULT CHARSET=utf8;
CREATE TABLE `tb_rkvcdukyuq` LIKE `tb_quilrfojmp`;
CREATE TABLE `tb_erkppdannn` (
  `col_zmjbwpsrnw` timestamp(3) NULL DEFAULT CURRENT_TIMESTAMP(3),
  `col_qqzmxlmztj` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8mb4 NULL DEFAULT 'enum_or_set_0',
  `col_hwnlilelpf` smallint unsigned DEFAULT '1',
  `col_oyeoemafea` date NULL DEFAULT '2019-07-04',
  CONSTRAINT `symb_biynehpjlu` UNIQUE (`col_hwnlilelpf`,`col_oyeoemafea`),
  CONSTRAINT `symb_yrommseaif` UNIQUE `uk_ztxuhpyaxo` (`col_zmjbwpsrnw`,`col_oyeoemafea`)
) DEFAULT CHARSET=utf8;
CREATE TABLE `tb_knsgjmsufx` LIKE `tb_odnffrvnvl`;
CREATE TABLE `tb_ljqeyzesru` LIKE `tb_rpuyybsdob`;
CREATE TABLE `tb_szlgabmztl` (
  `col_hfydymybbg` tinytext CHARACTER SET latin1,
  CONSTRAINT `symb_gpchhhvhwm` UNIQUE KEY (`col_hfydymybbg`(20))
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
RENAME TABLE `tb_qfmtewribz` TO `tb_qeokuaneue`;
RENAME TABLE `tb_lttgkqunzl` TO `tb_uhjgsigdfr`, `tb_erkppdannn` TO `tb_xeoclzosgr`;
RENAME TABLE `tb_quilrfojmp` TO `tb_hycsuwyoxr`, `tb_knsgjmsufx` TO `tb_bbvvzzczcw`;
RENAME TABLE `tb_xeoclzosgr` TO `tb_fxxptceefr`;
RENAME TABLE `tb_odnffrvnvl` TO `tb_rtnozmledi`;
RENAME TABLE `tb_fxxptceefr` TO `tb_tekgceqrmv`;
RENAME TABLE `tb_hycsuwyoxr` TO `tb_hcvvpyqbtd`, `tb_szlgabmztl` TO `tb_yfjonexaag`;
RENAME TABLE `tb_tekgceqrmv` TO `tb_wewsxxqmxd`, `tb_uhjgsigdfr` TO `tb_mbvuditpbx`;
DROP TABLE tb_mbvuditpbx, tb_rkvcdukyuq;
DROP TABLE tb_rtnozmledi, tb_qeokuaneue;
CREATE TABLE `tb_quilrfojmp` (
  `col_ruygkecjzp` longblob,
  `col_orycjbfrss` year DEFAULT '2019',
  `col_aqqnunnega` varbinary(225),
  `col_pxwauaruqw` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8 DEFAULT 'enum_or_set_0',
  UNIQUE `col_ruygkecjzp` (`col_ruygkecjzp`(14),`col_orycjbfrss`)
) DEFAULT CHARSET=utf8;
CREATE TABLE `tb_rpuyybsdob` (
  `col_qmvlxemsmt` datetime(6),
  `col_wcntsrfsjf` int NULL DEFAULT '1',
  `col_kekvofvtus` integer(70) unsigned DEFAULT '1',
  `col_bhbfokpfhm` float
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE TABLE `tb_qfmtewribz` (
  `col_hocmjkikme` int unsigned,
  `col_qbggcmwxyh` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8mb4 NOT NULL,
  `col_fulwqoqveo` mediumint unsigned zerofill,
  `col_eniifaecwj` integer(159) NOT NULL DEFAULT '1',
  PRIMARY KEY (`col_qbggcmwxyh`,`col_eniifaecwj`),
  UNIQUE INDEX `uk_fmvsmeqnst` (`col_fulwqoqveo`),
  CONSTRAINT `symb_xifnljgzcv` UNIQUE (`col_qbggcmwxyh`,`col_fulwqoqveo`)
) DEFAULT CHARSET=latin1;
CREATE TABLE `tb_odnffrvnvl` LIKE `tb_quilrfojmp`;
CREATE TABLE `tb_lttgkqunzl` (
  `col_nllztggwyd` smallint unsigned DEFAULT '1'
) DEFAULT CHARSET=utf8;
CREATE TABLE `tb_rkvcdukyuq` LIKE `tb_quilrfojmp`;
CREATE TABLE `tb_erkppdannn` (
  `col_zmjbwpsrnw` timestamp(3) NULL DEFAULT CURRENT_TIMESTAMP(3),
  `col_qqzmxlmztj` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8mb4 NULL DEFAULT 'enum_or_set_0',
  `col_hwnlilelpf` smallint unsigned DEFAULT '1',
  `col_oyeoemafea` date NULL DEFAULT '2019-07-04',
  CONSTRAINT `symb_biynehpjlu` UNIQUE (`col_hwnlilelpf`,`col_oyeoemafea`),
  CONSTRAINT `symb_yrommseaif` UNIQUE `uk_ztxuhpyaxo` (`col_zmjbwpsrnw`,`col_oyeoemafea`)
) DEFAULT CHARSET=utf8;
CREATE TABLE `tb_knsgjmsufx` LIKE `tb_odnffrvnvl`;
CREATE TABLE `tb_ljqeyzesru` LIKE `tb_rpuyybsdob`;
CREATE TABLE `tb_szlgabmztl` (
  `col_hfydymybbg` tinytext CHARACTER SET latin1,
  CONSTRAINT `symb_gpchhhvhwm` UNIQUE KEY (`col_hfydymybbg`(20))
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
RENAME TABLE `tb_qfmtewribz` TO `tb_qeokuaneue`;
RENAME TABLE `tb_lttgkqunzl` TO `tb_uhjgsigdfr`, `tb_erkppdannn` TO `tb_xeoclzosgr`;
RENAME TABLE `tb_quilrfojmp` TO `tb_hycsuwyoxr`, `tb_knsgjmsufx` TO `tb_bbvvzzczcw`;
RENAME TABLE `tb_xeoclzosgr` TO `tb_fxxptceefr`;
RENAME TABLE `tb_odnffrvnvl` TO `tb_rtnozmledi`;
RENAME TABLE `tb_fxxptceefr` TO `tb_tekgceqrmv`;
RENAME TABLE `tb_hycsuwyoxr` TO `tb_hcvvpyqbtd`, `tb_szlgabmztl` TO `tb_yfjonexaag`;
RENAME TABLE `tb_tekgceqrmv` TO `tb_wewsxxqmxd`, `tb_uhjgsigdfr` TO `tb_mbvuditpbx`;
DROP TABLE tb_mbvuditpbx, tb_rkvcdukyuq;
DROP TABLE tb_rtnozmledi, tb_qeokuaneue;
CREATE TABLE `tb_quilrfojmp` (
  `col_ruygkecjzp` longblob,
  `col_orycjbfrss` year DEFAULT '2019',
  `col_aqqnunnega` varbinary(225),
  `col_pxwauaruqw` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8 DEFAULT 'enum_or_set_0',
  UNIQUE `col_ruygkecjzp` (`col_ruygkecjzp`(14),`col_orycjbfrss`)
) DEFAULT CHARSET=utf8;
CREATE TABLE `tb_rpuyybsdob` (
  `col_qmvlxemsmt` datetime(6),
  `col_wcntsrfsjf` int NULL DEFAULT '1',
  `col_kekvofvtus` integer(70) unsigned DEFAULT '1',
  `col_bhbfokpfhm` float
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE TABLE `tb_qfmtewribz` (
  `col_hocmjkikme` int unsigned,
  `col_qbggcmwxyh` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8mb4 NOT NULL,
  `col_fulwqoqveo` mediumint unsigned zerofill,
  `col_eniifaecwj` integer(159) NOT NULL DEFAULT '1',
  PRIMARY KEY (`col_qbggcmwxyh`,`col_eniifaecwj`),
  UNIQUE INDEX `uk_fmvsmeqnst` (`col_fulwqoqveo`),
  CONSTRAINT `symb_xifnljgzcv` UNIQUE (`col_qbggcmwxyh`,`col_fulwqoqveo`)
) DEFAULT CHARSET=latin1;
CREATE TABLE `tb_odnffrvnvl` LIKE `tb_quilrfojmp`;
CREATE TABLE `tb_lttgkqunzl` (
  `col_nllztggwyd` smallint unsigned DEFAULT '1'
) DEFAULT CHARSET=utf8;
CREATE TABLE `tb_rkvcdukyuq` LIKE `tb_quilrfojmp`;
CREATE TABLE `tb_erkppdannn` (
  `col_zmjbwpsrnw` timestamp(3) NULL DEFAULT CURRENT_TIMESTAMP(3),
  `col_qqzmxlmztj` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8mb4 NULL DEFAULT 'enum_or_set_0',
  `col_hwnlilelpf` smallint unsigned DEFAULT '1',
  `col_oyeoemafea` date NULL DEFAULT '2019-07-04',
  CONSTRAINT `symb_biynehpjlu` UNIQUE (`col_hwnlilelpf`,`col_oyeoemafea`),
  CONSTRAINT `symb_yrommseaif` UNIQUE `uk_ztxuhpyaxo` (`col_zmjbwpsrnw`,`col_oyeoemafea`)
) DEFAULT CHARSET=utf8;
CREATE TABLE `tb_knsgjmsufx` LIKE `tb_odnffrvnvl`;
CREATE TABLE `tb_ljqeyzesru` LIKE `tb_rpuyybsdob`;
CREATE TABLE `tb_szlgabmztl` (
  `col_hfydymybbg` tinytext CHARACTER SET latin1,
  CONSTRAINT `symb_gpchhhvhwm` UNIQUE KEY (`col_hfydymybbg`(20))
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
RENAME TABLE `tb_qfmtewribz` TO `tb_qeokuaneue`;
RENAME TABLE `tb_lttgkqunzl` TO `tb_uhjgsigdfr`, `tb_erkppdannn` TO `tb_xeoclzosgr`;
RENAME TABLE `tb_quilrfojmp` TO `tb_hycsuwyoxr`, `tb_knsgjmsufx` TO `tb_bbvvzzczcw`;
RENAME TABLE `tb_xeoclzosgr` TO `tb_fxxptceefr`;
RENAME TABLE `tb_odnffrvnvl` TO `tb_rtnozmledi`;
RENAME TABLE `tb_fxxptceefr` TO `tb_tekgceqrmv`;
RENAME TABLE `tb_hycsuwyoxr` TO `tb_hcvvpyqbtd`, `tb_szlgabmztl` TO `tb_yfjonexaag`;
RENAME TABLE `tb_tekgceqrmv` TO `tb_wewsxxqmxd`, `tb_uhjgsigdfr` TO `tb_mbvuditpbx`;
DROP TABLE tb_mbvuditpbx, tb_rkvcdukyuq;
DROP TABLE tb_rtnozmledi, tb_qeokuaneue;
CREATE TABLE `tb_quilrfojmp` (
  `col_ruygkecjzp` longblob,
  `col_orycjbfrss` year DEFAULT '2019',
  `col_aqqnunnega` varbinary(225),
  `col_pxwauaruqw` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8 DEFAULT 'enum_or_set_0',
  UNIQUE `col_ruygkecjzp` (`col_ruygkecjzp`(14),`col_orycjbfrss`)
) DEFAULT CHARSET=utf8;
CREATE TABLE `tb_rpuyybsdob` (
  `col_qmvlxemsmt` datetime(6),
  `col_wcntsrfsjf` int NULL DEFAULT '1',
  `col_kekvofvtus` integer(70) unsigned DEFAULT '1',
  `col_bhbfokpfhm` float
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE TABLE `tb_qfmtewribz` (
  `col_hocmjkikme` int unsigned,
  `col_qbggcmwxyh` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8mb4 NOT NULL,
  `col_fulwqoqveo` mediumint unsigned zerofill,
  `col_eniifaecwj` integer(159) NOT NULL DEFAULT '1',
  PRIMARY KEY (`col_qbggcmwxyh`,`col_eniifaecwj`),
  UNIQUE INDEX `uk_fmvsmeqnst` (`col_fulwqoqveo`),
  CONSTRAINT `symb_xifnljgzcv` UNIQUE (`col_qbggcmwxyh`,`col_fulwqoqveo`)
) DEFAULT CHARSET=latin1;
CREATE TABLE `tb_odnffrvnvl` LIKE `tb_quilrfojmp`;
CREATE TABLE `tb_lttgkqunzl` (
  `col_nllztggwyd` smallint unsigned DEFAULT '1'
) DEFAULT CHARSET=utf8;
CREATE TABLE `tb_rkvcdukyuq` LIKE `tb_quilrfojmp`;
CREATE TABLE `tb_erkppdannn` (
  `col_zmjbwpsrnw` timestamp(3) NULL DEFAULT CURRENT_TIMESTAMP(3),
  `col_qqzmxlmztj` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8mb4 NULL DEFAULT 'enum_or_set_0',
  `col_hwnlilelpf` smallint unsigned DEFAULT '1',
  `col_oyeoemafea` date NULL DEFAULT '2019-07-04',
  CONSTRAINT `symb_biynehpjlu` UNIQUE (`col_hwnlilelpf`,`col_oyeoemafea`),
  CONSTRAINT `symb_yrommseaif` UNIQUE `uk_ztxuhpyaxo` (`col_zmjbwpsrnw`,`col_oyeoemafea`)
) DEFAULT CHARSET=utf8;
CREATE TABLE `tb_knsgjmsufx` LIKE `tb_odnffrvnvl`;
CREATE TABLE `tb_ljqeyzesru` LIKE `tb_rpuyybsdob`;
CREATE TABLE `tb_szlgabmztl` (
  `col_hfydymybbg` tinytext CHARACTER SET latin1,
  CONSTRAINT `symb_gpchhhvhwm` UNIQUE KEY (`col_hfydymybbg`(20))
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
RENAME TABLE `tb_qfmtewribz` TO `tb_qeokuaneue`;
RENAME TABLE `tb_lttgkqunzl` TO `tb_uhjgsigdfr`, `tb_erkppdannn` TO `tb_xeoclzosgr`;
RENAME TABLE `tb_quilrfojmp` TO `tb_hycsuwyoxr`, `tb_knsgjmsufx` TO `tb_bbvvzzczcw`;
RENAME TABLE `tb_xeoclzosgr` TO `tb_fxxptceefr`;
RENAME TABLE `tb_odnffrvnvl` TO `tb_rtnozmledi`;
RENAME TABLE `tb_fxxptceefr` TO `tb_tekgceqrmv`;
RENAME TABLE `tb_hycsuwyoxr` TO `tb_hcvvpyqbtd`, `tb_szlgabmztl` TO `tb_yfjonexaag`;
RENAME TABLE `tb_tekgceqrmv` TO `tb_wewsxxqmxd`, `tb_uhjgsigdfr` TO `tb_mbvuditpbx`;
DROP TABLE tb_mbvuditpbx, tb_rkvcdukyuq;
DROP TABLE tb_rtnozmledi, tb_qeokuaneue;
CREATE TABLE `tb_quilrfojmp` (
  `col_ruygkecjzp` longblob,
  `col_orycjbfrss` year DEFAULT '2019',
  `col_aqqnunnega` varbinary(225),
  `col_pxwauaruqw` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8 DEFAULT 'enum_or_set_0',
  UNIQUE `col_ruygkecjzp` (`col_ruygkecjzp`(14),`col_orycjbfrss`)
) DEFAULT CHARSET=utf8;
CREATE TABLE `tb_rpuyybsdob` (
  `col_qmvlxemsmt` datetime(6),
  `col_wcntsrfsjf` int NULL DEFAULT '1',
  `col_kekvofvtus` integer(70) unsigned DEFAULT '1',
  `col_bhbfokpfhm` float
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE TABLE `tb_qfmtewribz` (
  `col_hocmjkikme` int unsigned,
  `col_qbggcmwxyh` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8mb4 NOT NULL,
  `col_fulwqoqveo` mediumint unsigned zerofill,
  `col_eniifaecwj` integer(159) NOT NULL DEFAULT '1',
  PRIMARY KEY (`col_qbggcmwxyh`,`col_eniifaecwj`),
  UNIQUE INDEX `uk_fmvsmeqnst` (`col_fulwqoqveo`),
  CONSTRAINT `symb_xifnljgzcv` UNIQUE (`col_qbggcmwxyh`,`col_fulwqoqveo`)
) DEFAULT CHARSET=latin1;
CREATE TABLE `tb_odnffrvnvl` LIKE `tb_quilrfojmp`;
CREATE TABLE `tb_lttgkqunzl` (
  `col_nllztggwyd` smallint unsigned DEFAULT '1'
) DEFAULT CHARSET=utf8;
CREATE TABLE `tb_rkvcdukyuq` LIKE `tb_quilrfojmp`;
CREATE TABLE `tb_erkppdannn` (
  `col_zmjbwpsrnw` timestamp(3) NULL DEFAULT CURRENT_TIMESTAMP(3),
  `col_qqzmxlmztj` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8mb4 NULL DEFAULT 'enum_or_set_0',
  `col_hwnlilelpf` smallint unsigned DEFAULT '1',
  `col_oyeoemafea` date NULL DEFAULT '2019-07-04',
  CONSTRAINT `symb_biynehpjlu` UNIQUE (`col_hwnlilelpf`,`col_oyeoemafea`),
  CONSTRAINT `symb_yrommseaif` UNIQUE `uk_ztxuhpyaxo` (`col_zmjbwpsrnw`,`col_oyeoemafea`)
) DEFAULT CHARSET=utf8;
CREATE TABLE `tb_knsgjmsufx` LIKE `tb_odnffrvnvl`;
CREATE TABLE `tb_ljqeyzesru` LIKE `tb_rpuyybsdob`;
CREATE TABLE `tb_szlgabmztl` (
  `col_hfydymybbg` tinytext CHARACTER SET latin1,
  CONSTRAINT `symb_gpchhhvhwm` UNIQUE KEY (`col_hfydymybbg`(20))
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
RENAME TABLE `tb_qfmtewribz` TO `tb_qeokuaneue`;
RENAME TABLE `tb_lttgkqunzl` TO `tb_uhjgsigdfr`, `tb_erkppdannn` TO `tb_xeoclzosgr`;
RENAME TABLE `tb_quilrfojmp` TO `tb_hycsuwyoxr`, `tb_knsgjmsufx` TO `tb_bbvvzzczcw`;
RENAME TABLE `tb_xeoclzosgr` TO `tb_fxxptceefr`;
RENAME TABLE `tb_odnffrvnvl` TO `tb_rtnozmledi`;
RENAME TABLE `tb_fxxptceefr` TO `tb_tekgceqrmv`;
RENAME TABLE `tb_hycsuwyoxr` TO `tb_hcvvpyqbtd`, `tb_szlgabmztl` TO `tb_yfjonexaag`;
RENAME TABLE `tb_tekgceqrmv` TO `tb_wewsxxqmxd`, `tb_uhjgsigdfr` TO `tb_mbvuditpbx`;
DROP TABLE tb_mbvuditpbx, tb_rkvcdukyuq;
DROP TABLE tb_rtnozmledi, tb_qeokuaneue;
