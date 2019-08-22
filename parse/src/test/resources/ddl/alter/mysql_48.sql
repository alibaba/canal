CREATE TABLE `tb_fzlrvzctbr` (
  `col_fcxqhwmcni` longblob,
  `col_kswnnckixx` tinytext,
  `col_hpqddjdxxz` longblob,
  `col_xnreiiwrcm` mediumint(225) unsigned DEFAULT '1',
  UNIQUE KEY `uk_cvkkrimfqi` (`col_kswnnckixx`(2),`col_hpqddjdxxz`(29))
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE TABLE `tb_tmasbkkzbt` (
  `col_gvdetvdvvm` longtext,
  UNIQUE KEY `uk_dvgbfwphsv` (`col_gvdetvdvvm`(27))
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE TABLE `tb_vnbuhzbwvw` (
  `col_gttrfjbbyi` varchar(161) NOT NULL,
  `col_ksvxcquzhp` longtext CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  `col_ibahzyeiod` year(4) NOT NULL,
  PRIMARY KEY (`col_gttrfjbbyi`(8)),
  UNIQUE KEY `col_ksvxcquzhp` (`col_ksvxcquzhp`(16),`col_ibahzyeiod`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
