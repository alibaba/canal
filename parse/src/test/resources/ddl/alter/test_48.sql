CREATE TABLE `tb_cfbrttmxtt` (
  `col_fcxqhwmcni` longblob,
  `col_kswnnckixx` tinytext CHARACTER SET latin1,
  `col_hpqddjdxxz` longblob,
  `col_xnreiiwrcm` mediumint(225) unsigned DEFAULT '1',
  UNIQUE INDEX `uk_cvkkrimfqi` (`col_kswnnckixx`(2),`col_hpqddjdxxz`(29))
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE TABLE `tb_ibnzwrlofl` (
  `col_kxzbyijcwt` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT 'enum_or_set_0',
  `col_ydqlwiumbq` year NOT NULL,
  `col_ektspnhakb` decimal(56,19),
  PRIMARY KEY (`col_ydqlwiumbq`),
  UNIQUE `uk_wrgomwzzul` (`col_kxzbyijcwt`,`col_ektspnhakb`),
  UNIQUE `uk_ufhjbglflj` (`col_ektspnhakb`)
) DEFAULT CHARSET=latin1;
CREATE TABLE `tb_qhfiwjigtv` (
  `col_gttrfjbbyi` varchar(161) CHARACTER SET utf8 NOT NULL,
  `col_ksvxcquzhp` longtext CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  `col_ibahzyeiod` year(4) NOT NULL,
  CONSTRAINT PRIMARY KEY (`col_gttrfjbbyi`(8)),
  UNIQUE INDEX `col_ksvxcquzhp` (`col_ksvxcquzhp`(16),`col_ibahzyeiod`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
RENAME TABLE `tb_cfbrttmxtt` TO `tb_fzlrvzctbr`;
RENAME TABLE `tb_ibnzwrlofl` TO `tb_tmasbkkzbt`;
RENAME TABLE `tb_qhfiwjigtv` TO `tb_vnbuhzbwvw`;
ALTER TABLE `tb_tmasbkkzbt` ADD COLUMN `col_gvdetvdvvm` text(3429904651);
ALTER TABLE `tb_tmasbkkzbt` ADD UNIQUE `uk_dvgbfwphsv` (`col_gvdetvdvvm`(27));
ALTER TABLE `tb_tmasbkkzbt` ALTER `col_ektspnhakb` SET DEFAULT NULL;
ALTER TABLE `tb_tmasbkkzbt` DROP COLUMN `col_kxzbyijcwt`, DROP COLUMN `col_ektspnhakb`;
ALTER TABLE `tb_tmasbkkzbt` DROP `col_ydqlwiumbq`;
