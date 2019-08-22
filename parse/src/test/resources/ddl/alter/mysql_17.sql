CREATE TABLE `tb_ygyyvdctrs` (
  `col_hirzxaomit` varbinary(145) NOT NULL,
  `col_rpxmbgbwos` longblob,
  `col_emexlkeymz` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8mb4 DEFAULT 'enum_or_set_0',
  `col_ibmogxgtyp` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET latin1 DEFAULT 'enum_or_set_0',
  `col_yzbwkjwzqw` int(10) unsigned zerofill NOT NULL,
  `col_dmdpojzsct` varbinary(166) DEFAULT NULL,
  `col_oxgbfbgzov` double(112,2) DEFAULT NULL,
  `col_zkdwjtabnw` smallint(5) unsigned NOT NULL,
  `col_dxgsghrizm` tinyblob,
  `col_bowjbzwvyw` date DEFAULT NULL,
  `col_inneappfsm` varchar(112) CHARACTER SET utf8mb4 DEFAULT NULL,
  UNIQUE KEY `col_emexlkeymz` (`col_emexlkeymz`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
