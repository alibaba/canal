CREATE TABLE `tb_onrsvwgpqb` (
  `col_wsaejbacoe` longtext CHARACTER SET utf8,
  `col_wgrjfcezod` int(19) NOT NULL,
  `col_yhavwtiaep` date NOT NULL,
  `col_ezzupofzra` tinytext CHARACTER SET utf8mb4,
  UNIQUE KEY `col_wgrjfcezod` (`col_wgrjfcezod`),
  UNIQUE KEY `col_wsaejbacoe` (`col_wsaejbacoe`(29),`col_wgrjfcezod`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE TABLE `tb_pactwzqamv` (
  `col_hriruwciyw` longblob,
  UNIQUE KEY `col_hriruwciyw` (`col_hriruwciyw`(11)),
  UNIQUE KEY `uk_ktdzldgjqo` (`col_hriruwciyw`(18))
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE TABLE `tb_xfnqchameq` (
  `col_lsaozjvioc` tinyint(4) DEFAULT '1',
  `col_umquuzgoic` binary(1) DEFAULT NULL,
  `col_hssxpkdkzt` decimal(10,0) DEFAULT '1',
  UNIQUE KEY `uk_ribdoefndw` (`col_lsaozjvioc`),
  UNIQUE KEY `uk_ygjhgipibu` (`col_umquuzgoic`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
