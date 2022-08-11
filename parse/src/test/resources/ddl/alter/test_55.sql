CREATE TABLE `tb_jkbiibupis` (
  `col_fkolmhdrqv` int unsigned,
  `col_evazkiwccb` longblob,
  `col_gfzhcaruwf` tinytext CHARACTER SET utf8,
  UNIQUE KEY `col_evazkiwccb` (`col_evazkiwccb`(17)),
  UNIQUE KEY `uk_qeuoketlhl` (`col_gfzhcaruwf`(28))
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `tb_pqjlitgelb` (
  `col_rvdpillohr` smallint(51) unsigned NOT NULL,
  `col_wjvgjpqbgz` smallint unsigned zerofill NULL,
  CONSTRAINT symb_vytyeftcau PRIMARY KEY (`col_rvdpillohr`),
  UNIQUE INDEX `uk_aqetlmillq` (`col_wjvgjpqbgz`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
RENAME TABLE `tb_jkbiibupis` TO `tb_epubreeayc`, `tb_pqjlitgelb` TO `tb_uafierhjhg`;
ALTER TABLE `tb_epubreeayc` ADD COLUMN `col_eznnzdmtjb` blob(2459218563) AFTER `col_gfzhcaruwf`;
ALTER TABLE `tb_epubreeayc` ADD `col_orujerqezb` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8mb4;
ALTER TABLE `tb_epubreeayc` ADD COLUMN (`col_furvidozht` text(2634346399) CHARACTER SET utf8, `col_embnqthyuu` float(30) NOT NULL);
ALTER TABLE `tb_epubreeayc` ADD (`col_nifvahqfqx` tinytext CHARACTER SET utf8mb4, `col_shqnxczemy` binary(149));
ALTER TABLE `tb_epubreeayc` ADD `col_vkrzrlbckh` binary AFTER `col_embnqthyuu`;
ALTER TABLE `tb_epubreeayc` ADD COLUMN `col_aqvohzuvxd` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8mb4 NOT NULL;
ALTER TABLE `tb_epubreeayc` ADD (`col_aexcfuzeln` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') NULL DEFAULT 'enum_or_set_0', `col_gyjfdfpemu` text(1480656314));
ALTER TABLE `tb_epubreeayc` DEFAULT CHARACTER SET = utf8mb4;
ALTER TABLE `tb_epubreeayc` ADD PRIMARY KEY (`col_embnqthyuu`);
ALTER TABLE `tb_epubreeayc` ADD UNIQUE `uk_uydztaglgb` (`col_aqvohzuvxd`,`col_aexcfuzeln`);
ALTER TABLE `tb_epubreeayc` DROP `col_furvidozht`;
ALTER TABLE `tb_epubreeayc` DROP `col_vkrzrlbckh`;
ALTER TABLE `tb_epubreeayc` DROP `col_gfzhcaruwf`;
ALTER TABLE `tb_epubreeayc` DROP `col_aqvohzuvxd`;
ALTER TABLE `tb_epubreeayc` DROP COLUMN `col_orujerqezb`;
ALTER TABLE `tb_epubreeayc` DROP `col_nifvahqfqx`, DROP `col_eznnzdmtjb`;
ALTER TABLE `tb_epubreeayc` DROP COLUMN `col_aexcfuzeln`, DROP COLUMN `col_evazkiwccb`;
ALTER TABLE `tb_epubreeayc` DROP `col_fkolmhdrqv`;
