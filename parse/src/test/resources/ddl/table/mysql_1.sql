CREATE TABLE `tb_bbvvzzczcw` (
  `col_ruygkecjzp` longblob,
  `col_orycjbfrss` year(4) DEFAULT '2019',
  `col_aqqnunnega` varbinary(225) DEFAULT NULL,
  `col_pxwauaruqw` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') DEFAULT 'enum_or_set_0',
  UNIQUE KEY `col_ruygkecjzp` (`col_ruygkecjzp`(14),`col_orycjbfrss`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `tb_hcvvpyqbtd` (
  `col_ruygkecjzp` longblob,
  `col_orycjbfrss` year(4) DEFAULT '2019',
  `col_aqqnunnega` varbinary(225) DEFAULT NULL,
  `col_pxwauaruqw` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') DEFAULT 'enum_or_set_0',
  UNIQUE KEY `col_ruygkecjzp` (`col_ruygkecjzp`(14),`col_orycjbfrss`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `tb_ljqeyzesru` (
  `col_qmvlxemsmt` datetime(6) DEFAULT NULL,
  `col_wcntsrfsjf` int(11) DEFAULT '1',
  `col_kekvofvtus` int(70) unsigned DEFAULT '1',
  `col_bhbfokpfhm` float DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE TABLE `tb_rpuyybsdob` (
  `col_qmvlxemsmt` datetime(6) DEFAULT NULL,
  `col_wcntsrfsjf` int(11) DEFAULT '1',
  `col_kekvofvtus` int(70) unsigned DEFAULT '1',
  `col_bhbfokpfhm` float DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE TABLE `tb_wewsxxqmxd` (
  `col_zmjbwpsrnw` timestamp(3) NULL DEFAULT CURRENT_TIMESTAMP(3),
  `col_qqzmxlmztj` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8mb4 DEFAULT 'enum_or_set_0',
  `col_hwnlilelpf` smallint(5) unsigned DEFAULT '1',
  `col_oyeoemafea` date DEFAULT '2019-07-04',
  UNIQUE KEY `symb_biynehpjlu` (`col_hwnlilelpf`,`col_oyeoemafea`),
  UNIQUE KEY `uk_ztxuhpyaxo` (`col_zmjbwpsrnw`,`col_oyeoemafea`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `tb_yfjonexaag` (
  `col_hfydymybbg` tinytext,
  UNIQUE KEY `symb_gpchhhvhwm` (`col_hfydymybbg`(20))
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
