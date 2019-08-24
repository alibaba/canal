CREATE TABLE `tb_iuqrkykntm` (
  `col_vqbxwxbdco` float(53) NULL,
  `col_axuvjogcci` datetime DEFAULT '2019-07-04 00:00:00',
  `col_rrrdiozgxi` mediumtext CHARACTER SET latin1,
  `col_hjaoyhupuv` varchar(176) CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  UNIQUE `col_rrrdiozgxi` (`col_rrrdiozgxi`(23)),
  UNIQUE KEY `uk_gscqcjtlyg` (`col_hjaoyhupuv`(13))
) DEFAULT CHARSET=latin1;
CREATE TABLE `tb_fkndvyyooo` LIKE `tb_iuqrkykntm`;
RENAME TABLE `tb_iuqrkykntm` TO `tb_gwhacmapbj`;
RENAME TABLE `tb_gwhacmapbj` TO `tb_havvvaohnr`;
RENAME TABLE `tb_havvvaohnr` TO `tb_nduadhjfex`;
DROP TABLE tb_nduadhjfex, tb_fkndvyyooo;
CREATE TABLE `tb_tilukdjjhc` (
  `col_kxelvjyosf` int(150),
  `col_laleouwpiw` int NOT NULL DEFAULT '1',
  PRIMARY KEY (`col_laleouwpiw`),
  UNIQUE `uk_hkgebfxhma` (`col_kxelvjyosf`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
ALTER TABLE `tb_tilukdjjhc` ADD `col_fgidnnqvqm` year NOT NULL;
ALTER TABLE `tb_tilukdjjhc` ADD COLUMN `col_loxvzbzvge` char CHARACTER SET utf8 NULL AFTER `col_kxelvjyosf`;
ALTER TABLE `tb_tilukdjjhc` ADD COLUMN (`col_uzxvkpgggz` mediumblob, `col_gqssffnbgo` tinyblob);
ALTER TABLE `tb_tilukdjjhc` ADD `col_jtyftybbku` decimal(13,5) NOT NULL AFTER `col_uzxvkpgggz`;
ALTER TABLE `tb_tilukdjjhc` ADD COLUMN `col_drlfahzqxk` datetime(4) NOT NULL;
ALTER TABLE `tb_tilukdjjhc` ADD (`col_bzzxemkcuj` double, `col_olnmovdyfw` bit(3) NULL DEFAULT b'0');
ALTER TABLE `tb_tilukdjjhc` ADD COLUMN `col_avfiwopsik` float;
ALTER TABLE `tb_tilukdjjhc` DEFAULT CHARACTER SET utf8mb4;
ALTER TABLE `tb_tilukdjjhc` ALTER COLUMN `col_bzzxemkcuj` DROP DEFAULT;
ALTER TABLE `tb_tilukdjjhc` CHANGE COLUMN `col_gqssffnbgo` `col_pmqkhijpcr` varbinary(71) NULL DEFAULT '\0' AFTER `col_fgidnnqvqm`;
ALTER TABLE `tb_tilukdjjhc` CHANGE `col_drlfahzqxk` `col_idhlrzdwud` char CHARACTER SET utf8 NOT NULL AFTER `col_avfiwopsik`;
ALTER TABLE `tb_tilukdjjhc` CHANGE COLUMN `col_avfiwopsik` `col_kcysycgbek` blob(2086586212);
ALTER TABLE `tb_tilukdjjhc` DROP COLUMN `col_bzzxemkcuj`;
ALTER TABLE `tb_tilukdjjhc` DROP PRIMARY KEY;
ALTER TABLE `tb_tilukdjjhc` DROP KEY `uk_hkgebfxhma`;
