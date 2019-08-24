CREATE TABLE `tb_walklosuks` (
  `col_lxbxpuuwyo` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  `col_rtbkufevvv` smallint zerofill
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
RENAME TABLE `tb_walklosuks` TO `tb_mkeptukxss`;
RENAME TABLE `tb_mkeptukxss` TO `tb_sqndbrgkvj`;
ALTER TABLE `tb_sqndbrgkvj` ADD (`col_xuafealoty` float, `col_durclethxo` smallint);
ALTER TABLE `tb_sqndbrgkvj` ADD `col_plylqkvjll` time(5);
ALTER TABLE `tb_sqndbrgkvj` ADD COLUMN `col_erbwiiktwe` integer zerofill NULL FIRST;
ALTER TABLE `tb_sqndbrgkvj` ADD (`col_pymdrmukax` float(32), `col_zamcrcjxwy` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT 'enum_or_set_0');
ALTER TABLE `tb_sqndbrgkvj` ADD COLUMN (`col_twjprfcscz` tinytext CHARACTER SET utf8 COLLATE utf8_unicode_ci, `col_zuurdmqnmy` tinytext CHARACTER SET utf8mb4);
ALTER TABLE `tb_sqndbrgkvj` ADD `col_mueqvldinf` integer unsigned DEFAULT '1' FIRST;
ALTER TABLE `tb_sqndbrgkvj` ADD `col_prvruzvlqp` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL;
ALTER TABLE `tb_sqndbrgkvj` ADD COLUMN `col_bujrefmxrj` bit(35) AFTER `col_xuafealoty`;
ALTER TABLE `tb_sqndbrgkvj` CHARACTER SET utf8mb4;
ALTER TABLE `tb_sqndbrgkvj` ADD CONSTRAINT symb_uyzanxlotu PRIMARY KEY (`col_prvruzvlqp`);
ALTER TABLE `tb_sqndbrgkvj` ADD UNIQUE INDEX (`col_plylqkvjll`,`col_pymdrmukax`);
ALTER TABLE `tb_sqndbrgkvj` ADD UNIQUE KEY `col_rtbkufevvv`(`col_rtbkufevvv`,`col_bujrefmxrj`);
ALTER TABLE `tb_sqndbrgkvj` CHANGE `col_durclethxo` `col_sgkaxgwwff` tinyint(123) NOT NULL AFTER `col_lxbxpuuwyo`;
ALTER TABLE `tb_sqndbrgkvj` DROP `col_zamcrcjxwy`;
ALTER TABLE `tb_sqndbrgkvj` DROP `col_bujrefmxrj`;
ALTER TABLE `tb_sqndbrgkvj` DROP COLUMN `col_sgkaxgwwff`;
ALTER TABLE `tb_sqndbrgkvj` DROP `col_erbwiiktwe`, DROP `col_zuurdmqnmy`;
ALTER TABLE `tb_sqndbrgkvj` DROP COLUMN `col_twjprfcscz`, DROP COLUMN `col_lxbxpuuwyo`;
ALTER TABLE `tb_sqndbrgkvj` DROP PRIMARY KEY;
ALTER TABLE `tb_sqndbrgkvj` DROP KEY `col_rtbkufevvv`;
