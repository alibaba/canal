CREATE TABLE `tb_nyxoslfyhc` (
  `col_nzrjgagsiy` smallint(162) unsigned NULL DEFAULT '1'
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE TABLE `tb_psucbscimn` (
  `col_uhvvohvmuw` text(1272911620) CHARACTER SET utf8mb4,
  UNIQUE `uk_ggqkubbmbr` (`col_uhvvohvmuw`(21)),
  CONSTRAINT `symb_esejgusggh` UNIQUE (`col_uhvvohvmuw`(32))
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `tb_wbrbzvpfra` (
  `col_lokvnorvsp` char(235) CHARACTER SET utf8mb4,
  `col_vazlbmjjtl` char CHARACTER SET latin1,
  `col_kcxzselokz` mediumint(229) unsigned NOT NULL DEFAULT '1',
  `col_bjiezlcrgf` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET latin1 NOT NULL DEFAULT 'enum_or_set_0',
  PRIMARY KEY (`col_kcxzselokz`,`col_bjiezlcrgf`),
  CONSTRAINT `symb_ydlysvxwrf` UNIQUE `uk_qxibrmyivk` (`col_bjiezlcrgf`),
  UNIQUE `uk_zmxfbsrjpe` (`col_bjiezlcrgf`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE TABLE `tb_ztljrixvrw` (
  `col_xqoffthmnd` time(4) NOT NULL,
  `col_aurisjtkrj` time DEFAULT '00:00:00',
  PRIMARY KEY (`col_xqoffthmnd`),
  CONSTRAINT UNIQUE INDEX `col_xqoffthmnd` (`col_xqoffthmnd`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE TABLE `tb_nsgdkkqgmo` (
  `col_dmhcmluntp` smallint
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
RENAME TABLE `tb_nsgdkkqgmo` TO `tb_eeqakoxfof`;
DROP TABLE tb_nyxoslfyhc;
DROP TABLE tb_eeqakoxfof;
DROP TABLE tb_ztljrixvrw, tb_psucbscimn;
