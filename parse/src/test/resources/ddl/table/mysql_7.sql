CREATE TABLE `tb_wpozpvwepq` (
  `col_mhnfdjvvcl` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') DEFAULT 'enum_or_set_0',
  `col_nppvqejmds` mediumint(8) unsigned zerofill DEFAULT NULL,
  UNIQUE KEY `uk_cgzmixhhpp` (`col_mhnfdjvvcl`),
  UNIQUE KEY `col_mhnfdjvvcl` (`col_mhnfdjvvcl`,`col_nppvqejmds`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
