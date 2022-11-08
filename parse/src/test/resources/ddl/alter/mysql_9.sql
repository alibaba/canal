CREATE TABLE `tb_sqndbrgkvj` (
  `col_mueqvldinf` int(10) unsigned DEFAULT '1',
  `col_rtbkufevvv` smallint(5) unsigned zerofill DEFAULT NULL,
  `col_xuafealoty` float DEFAULT NULL,
  `col_plylqkvjll` time(5) DEFAULT NULL,
  `col_pymdrmukax` double DEFAULT NULL,
  `col_prvruzvlqp` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  UNIQUE KEY `col_plylqkvjll` (`col_plylqkvjll`,`col_pymdrmukax`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
