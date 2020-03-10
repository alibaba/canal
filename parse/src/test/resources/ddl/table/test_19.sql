CREATE TABLE `tb_imaygfshnt` (
  `col_awltnvurel` numeric NULL DEFAULT '1',
  `col_wbacbcxgvc` longblob,
  `col_bewighnliy` double DEFAULT '1'
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE TABLE `tb_glxbnnkcoa` (
  `col_ahlnhmqsql` text CHARACTER SET utf8mb4,
  `col_blmlqemvsa` mediumblob,
  `col_riziohykgq` decimal(9,3) NOT NULL,
  `col_tywdbnerjb` integer(182) unsigned,
  CONSTRAINT PRIMARY KEY (`col_riziohykgq`)
) DEFAULT CHARSET=latin1;
CREATE TABLE `tb_tlrqmnicwr` LIKE `tb_imaygfshnt`;
CREATE TABLE `tb_zxnzzezzka` LIKE `tb_imaygfshnt`;
CREATE TABLE `tb_rfagtqwznu` (
  `col_qittiqcnvc` bit(44) DEFAULT b'0'
) DEFAULT CHARSET=latin1;
CREATE TABLE `tb_dlyfeqiynq` (
  `col_fwwfammsnr` tinytext CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  `col_hpjlwdunhe` time,
  CONSTRAINT `symb_hmboygosdg` UNIQUE (`col_fwwfammsnr`(31),`col_hpjlwdunhe`),
  CONSTRAINT `symb_jtwmljwpgc` UNIQUE INDEX `uk_yehobqqoum` (`col_fwwfammsnr`(17))
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `tb_feyelhrmnv` (
  `col_uzkloskrcr` decimal(14,3) NOT NULL,
  `col_cmxkwhehyk` blob(1976160958),
  PRIMARY KEY (`col_uzkloskrcr`),
  CONSTRAINT `symb_jphlrjgdei` UNIQUE INDEX `uk_smobcykuao` (`col_cmxkwhehyk`(25))
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE TABLE `tb_sxvecelfyb` (
  `col_zoezqbning` tinyblob,
  `col_fwuxxxpkrm` char CHARACTER SET utf8 NOT NULL,
  `col_npwkcywzey` integer(248) zerofill NULL,
  `col_apmomgcnou` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8 NULL,
  PRIMARY KEY (`col_fwuxxxpkrm`),
  CONSTRAINT UNIQUE KEY `uk_hchijvhjkj` (`col_fwuxxxpkrm`,`col_apmomgcnou`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `tb_sakzquypth` (
  `col_onjzrgbzvg` smallint zerofill,
  `col_eyswydvmwr` binary(217) NULL,
  `col_ebwgvtfuic` integer unsigned DEFAULT '1',
  `col_qndkhtfaqm` tinyblob,
  CONSTRAINT UNIQUE `uk_ktgusajomp` (`col_eyswydvmwr`(28),`col_ebwgvtfuic`),
  UNIQUE INDEX `col_ebwgvtfuic` (`col_ebwgvtfuic`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE TABLE `tb_zbvwgnjidh` (
  `col_xzormknbla` int unsigned NOT NULL,
  `col_lizstxeuhj` time(4),
  `col_kmgfxzwfbv` time(1),
  UNIQUE KEY `col_kmgfxzwfbv` (`col_kmgfxzwfbv`),
  UNIQUE `col_kmgfxzwfbv_2` (`col_kmgfxzwfbv`)
) DEFAULT CHARSET=utf8;
RENAME TABLE `tb_zxnzzezzka` TO `tb_rrvwpvzswg`;
RENAME TABLE `tb_dlyfeqiynq` TO `tb_knvlrbhvxs`;
RENAME TABLE `tb_zbvwgnjidh` TO `tb_ruhyyyifba`, `tb_glxbnnkcoa` TO `tb_kfpeuwjwzc`;
RENAME TABLE `tb_kfpeuwjwzc` TO `tb_zsrskpxhdj`, `tb_feyelhrmnv` TO `tb_uvinkyihqt`;
RENAME TABLE `tb_imaygfshnt` TO `tb_kvaptpdurn`, `tb_rfagtqwznu` TO `tb_fycvzpnnaa`;
RENAME TABLE `tb_zsrskpxhdj` TO `tb_tjoadmhvhc`;
RENAME TABLE `tb_tjoadmhvhc` TO `tb_tvjqzqyunz`, `tb_kvaptpdurn` TO `tb_rjbzsfvdte`;
RENAME TABLE `tb_rrvwpvzswg` TO `tb_gaubnuxkyk`;
RENAME TABLE `tb_uvinkyihqt` TO `tb_dmkotfedww`;
RENAME TABLE `tb_tlrqmnicwr` TO `tb_vlakuowlmg`, `tb_gaubnuxkyk` TO `tb_qfryzntbot`;
DROP TABLE tb_tvjqzqyunz, tb_dmkotfedww;
DROP TABLE tb_vlakuowlmg;
DROP TABLE tb_fycvzpnnaa, tb_knvlrbhvxs;
DROP TABLE tb_sxvecelfyb, tb_sakzquypth;
CREATE TABLE `tb_imaygfshnt` (
  `col_awltnvurel` numeric NULL DEFAULT '1',
  `col_wbacbcxgvc` longblob,
  `col_bewighnliy` double DEFAULT '1'
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE TABLE `tb_glxbnnkcoa` (
  `col_ahlnhmqsql` text CHARACTER SET utf8mb4,
  `col_blmlqemvsa` mediumblob,
  `col_riziohykgq` decimal(9,3) NOT NULL,
  `col_tywdbnerjb` integer(182) unsigned,
  CONSTRAINT PRIMARY KEY (`col_riziohykgq`)
) DEFAULT CHARSET=latin1;
CREATE TABLE `tb_tlrqmnicwr` LIKE `tb_imaygfshnt`;
CREATE TABLE `tb_zxnzzezzka` LIKE `tb_imaygfshnt`;
CREATE TABLE `tb_rfagtqwznu` (
  `col_qittiqcnvc` bit(44) DEFAULT b'0'
) DEFAULT CHARSET=latin1;
CREATE TABLE `tb_dlyfeqiynq` (
  `col_fwwfammsnr` tinytext CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  `col_hpjlwdunhe` time,
  CONSTRAINT `symb_hmboygosdg` UNIQUE (`col_fwwfammsnr`(31),`col_hpjlwdunhe`),
  CONSTRAINT `symb_jtwmljwpgc` UNIQUE INDEX `uk_yehobqqoum` (`col_fwwfammsnr`(17))
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `tb_feyelhrmnv` (
  `col_uzkloskrcr` decimal(14,3) NOT NULL,
  `col_cmxkwhehyk` blob(1976160958),
  PRIMARY KEY (`col_uzkloskrcr`),
  CONSTRAINT `symb_jphlrjgdei` UNIQUE INDEX `uk_smobcykuao` (`col_cmxkwhehyk`(25))
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE TABLE `tb_sxvecelfyb` (
  `col_zoezqbning` tinyblob,
  `col_fwuxxxpkrm` char CHARACTER SET utf8 NOT NULL,
  `col_npwkcywzey` integer(248) zerofill NULL,
  `col_apmomgcnou` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8 NULL,
  PRIMARY KEY (`col_fwuxxxpkrm`),
  CONSTRAINT UNIQUE KEY `uk_hchijvhjkj` (`col_fwuxxxpkrm`,`col_apmomgcnou`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `tb_sakzquypth` (
  `col_onjzrgbzvg` smallint zerofill,
  `col_eyswydvmwr` binary(217) NULL,
  `col_ebwgvtfuic` integer unsigned DEFAULT '1',
  `col_qndkhtfaqm` tinyblob,
  CONSTRAINT UNIQUE `uk_ktgusajomp` (`col_eyswydvmwr`(28),`col_ebwgvtfuic`),
  UNIQUE INDEX `col_ebwgvtfuic` (`col_ebwgvtfuic`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE TABLE `tb_zbvwgnjidh` (
  `col_xzormknbla` int unsigned NOT NULL,
  `col_lizstxeuhj` time(4),
  `col_kmgfxzwfbv` time(1),
  UNIQUE KEY `col_kmgfxzwfbv` (`col_kmgfxzwfbv`),
  UNIQUE `col_kmgfxzwfbv_2` (`col_kmgfxzwfbv`)
) DEFAULT CHARSET=utf8;
RENAME TABLE `tb_zxnzzezzka` TO `tb_rrvwpvzswg`;
RENAME TABLE `tb_dlyfeqiynq` TO `tb_knvlrbhvxs`;
RENAME TABLE `tb_zbvwgnjidh` TO `tb_ruhyyyifba`, `tb_glxbnnkcoa` TO `tb_kfpeuwjwzc`;
RENAME TABLE `tb_kfpeuwjwzc` TO `tb_zsrskpxhdj`, `tb_feyelhrmnv` TO `tb_uvinkyihqt`;
RENAME TABLE `tb_imaygfshnt` TO `tb_kvaptpdurn`, `tb_rfagtqwznu` TO `tb_fycvzpnnaa`;
RENAME TABLE `tb_zsrskpxhdj` TO `tb_tjoadmhvhc`;
RENAME TABLE `tb_tjoadmhvhc` TO `tb_tvjqzqyunz`, `tb_kvaptpdurn` TO `tb_rjbzsfvdte`;
RENAME TABLE `tb_rrvwpvzswg` TO `tb_gaubnuxkyk`;
RENAME TABLE `tb_uvinkyihqt` TO `tb_dmkotfedww`;
RENAME TABLE `tb_tlrqmnicwr` TO `tb_vlakuowlmg`, `tb_gaubnuxkyk` TO `tb_qfryzntbot`;
DROP TABLE tb_tvjqzqyunz, tb_dmkotfedww;
DROP TABLE tb_vlakuowlmg;
DROP TABLE tb_fycvzpnnaa, tb_knvlrbhvxs;
DROP TABLE tb_sxvecelfyb, tb_sakzquypth;
CREATE TABLE `tb_imaygfshnt` (
  `col_awltnvurel` numeric NULL DEFAULT '1',
  `col_wbacbcxgvc` longblob,
  `col_bewighnliy` double DEFAULT '1'
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE TABLE `tb_glxbnnkcoa` (
  `col_ahlnhmqsql` text CHARACTER SET utf8mb4,
  `col_blmlqemvsa` mediumblob,
  `col_riziohykgq` decimal(9,3) NOT NULL,
  `col_tywdbnerjb` integer(182) unsigned,
  CONSTRAINT PRIMARY KEY (`col_riziohykgq`)
) DEFAULT CHARSET=latin1;
CREATE TABLE `tb_tlrqmnicwr` LIKE `tb_imaygfshnt`;
CREATE TABLE `tb_zxnzzezzka` LIKE `tb_imaygfshnt`;
CREATE TABLE `tb_rfagtqwznu` (
  `col_qittiqcnvc` bit(44) DEFAULT b'0'
) DEFAULT CHARSET=latin1;
CREATE TABLE `tb_dlyfeqiynq` (
  `col_fwwfammsnr` tinytext CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  `col_hpjlwdunhe` time,
  CONSTRAINT `symb_hmboygosdg` UNIQUE (`col_fwwfammsnr`(31),`col_hpjlwdunhe`),
  CONSTRAINT `symb_jtwmljwpgc` UNIQUE INDEX `uk_yehobqqoum` (`col_fwwfammsnr`(17))
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `tb_feyelhrmnv` (
  `col_uzkloskrcr` decimal(14,3) NOT NULL,
  `col_cmxkwhehyk` blob(1976160958),
  PRIMARY KEY (`col_uzkloskrcr`),
  CONSTRAINT `symb_jphlrjgdei` UNIQUE INDEX `uk_smobcykuao` (`col_cmxkwhehyk`(25))
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE TABLE `tb_sxvecelfyb` (
  `col_zoezqbning` tinyblob,
  `col_fwuxxxpkrm` char CHARACTER SET utf8 NOT NULL,
  `col_npwkcywzey` integer(248) zerofill NULL,
  `col_apmomgcnou` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8 NULL,
  PRIMARY KEY (`col_fwuxxxpkrm`),
  CONSTRAINT UNIQUE KEY `uk_hchijvhjkj` (`col_fwuxxxpkrm`,`col_apmomgcnou`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `tb_sakzquypth` (
  `col_onjzrgbzvg` smallint zerofill,
  `col_eyswydvmwr` binary(217) NULL,
  `col_ebwgvtfuic` integer unsigned DEFAULT '1',
  `col_qndkhtfaqm` tinyblob,
  CONSTRAINT UNIQUE `uk_ktgusajomp` (`col_eyswydvmwr`(28),`col_ebwgvtfuic`),
  UNIQUE INDEX `col_ebwgvtfuic` (`col_ebwgvtfuic`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE TABLE `tb_zbvwgnjidh` (
  `col_xzormknbla` int unsigned NOT NULL,
  `col_lizstxeuhj` time(4),
  `col_kmgfxzwfbv` time(1),
  UNIQUE KEY `col_kmgfxzwfbv` (`col_kmgfxzwfbv`),
  UNIQUE `col_kmgfxzwfbv_2` (`col_kmgfxzwfbv`)
) DEFAULT CHARSET=utf8;
RENAME TABLE `tb_zxnzzezzka` TO `tb_rrvwpvzswg`;
RENAME TABLE `tb_dlyfeqiynq` TO `tb_knvlrbhvxs`;
RENAME TABLE `tb_zbvwgnjidh` TO `tb_ruhyyyifba`, `tb_glxbnnkcoa` TO `tb_kfpeuwjwzc`;
RENAME TABLE `tb_kfpeuwjwzc` TO `tb_zsrskpxhdj`, `tb_feyelhrmnv` TO `tb_uvinkyihqt`;
RENAME TABLE `tb_imaygfshnt` TO `tb_kvaptpdurn`, `tb_rfagtqwznu` TO `tb_fycvzpnnaa`;
RENAME TABLE `tb_zsrskpxhdj` TO `tb_tjoadmhvhc`;
RENAME TABLE `tb_tjoadmhvhc` TO `tb_tvjqzqyunz`, `tb_kvaptpdurn` TO `tb_rjbzsfvdte`;
RENAME TABLE `tb_rrvwpvzswg` TO `tb_gaubnuxkyk`;
RENAME TABLE `tb_uvinkyihqt` TO `tb_dmkotfedww`;
RENAME TABLE `tb_tlrqmnicwr` TO `tb_vlakuowlmg`, `tb_gaubnuxkyk` TO `tb_qfryzntbot`;
DROP TABLE tb_tvjqzqyunz, tb_dmkotfedww;
DROP TABLE tb_vlakuowlmg;
DROP TABLE tb_fycvzpnnaa, tb_knvlrbhvxs;
DROP TABLE tb_sxvecelfyb, tb_sakzquypth;
