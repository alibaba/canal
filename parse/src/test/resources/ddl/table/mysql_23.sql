CREATE TABLE `tb_gvxwgsxgte` (
  `col_quthabfydl` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `col_bpzwhwubra` int(118) unsigned DEFAULT '1',
  `col_fpkljodgme` time DEFAULT NULL,
  `col_zxdapdqzmv` blob,
  UNIQUE KEY `uk_vuzkhjajdc` (`col_bpzwhwubra`,`col_fpkljodgme`),
  UNIQUE KEY `uk_zknetkwpoq` (`col_fpkljodgme`,`col_zxdapdqzmv`(28))
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE TABLE `tb_oytmynjdbq` (
  `col_thwosnwagl` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8 DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE TABLE `tb_wtwwuhhmqp` (
  `col_aiifakdklr` datetime(6) DEFAULT NULL,
  `col_ulzkdgcmmu` time DEFAULT NULL,
  UNIQUE KEY `uk_ikryxrhfhx` (`col_aiifakdklr`),
  UNIQUE KEY `uk_ckiseqvinv` (`col_aiifakdklr`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
