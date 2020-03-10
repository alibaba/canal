CREATE TABLE `tb_ytaiteijaz` (
  `col_lxtrjmhpmr` year(4) DEFAULT NULL,
  `col_wvvanapjlz` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `col_xnljwkzjad` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') DEFAULT 'enum_or_set_0',
  `col_kajgusvxkq` smallint(75) DEFAULT NULL,
  `col_pwojmgycov` bigint(20) unsigned zerofill DEFAULT NULL,
  UNIQUE KEY `uk_pffwfanush` (`col_wvvanapjlz`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
