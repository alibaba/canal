CREATE TABLE `tb_ezaysnxloa` (
  `col_koftcyrhcl` int(233) DEFAULT '1',
  `col_nacwazcyzi` varbinary(12),
  UNIQUE `col_koftcyrhcl` (`col_koftcyrhcl`)
) DEFAULT CHARSET=utf8;
RENAME TABLE `tb_ezaysnxloa` TO `tb_wsxpqlbxhr`;
ALTER TABLE `tb_wsxpqlbxhr` ADD COLUMN (`col_nnckvjhnnd` tinytext CHARACTER SET utf8 COLLATE utf8_unicode_ci, `col_sxwmoaghtk` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') DEFAULT 'enum_or_set_0');
ALTER TABLE `tb_wsxpqlbxhr` ADD `col_btyjfvfohb` double(18,12);
ALTER TABLE `tb_wsxpqlbxhr` ADD (`col_udrjznhpgf` tinytext, `col_ynwgneexdq` mediumtext CHARACTER SET utf8 COLLATE utf8_unicode_ci);
ALTER TABLE `tb_wsxpqlbxhr` ADD COLUMN `col_kkdtlacxiy` char NOT NULL FIRST;
ALTER TABLE `tb_wsxpqlbxhr` ADD `col_xlcidwqtlg` tinytext CHARACTER SET utf8;
ALTER TABLE `tb_wsxpqlbxhr` ADD COLUMN (`col_huxsdgntaw` numeric(27,14) NOT NULL, `col_siklbsrsfa` date NOT NULL);
ALTER TABLE `tb_wsxpqlbxhr` ADD PRIMARY KEY (`col_kkdtlacxiy`,`col_huxsdgntaw`);
ALTER TABLE `tb_wsxpqlbxhr` ADD UNIQUE INDEX `uk_cpzrxvccoo` (`col_nacwazcyzi`(7),`col_xlcidwqtlg`(9));
ALTER TABLE `tb_wsxpqlbxhr` CHANGE COLUMN `col_udrjznhpgf` `col_qpusrblupw` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') DEFAULT 'enum_or_set_0';
ALTER TABLE `tb_wsxpqlbxhr` DROP COLUMN `col_huxsdgntaw`;
ALTER TABLE `tb_wsxpqlbxhr` DROP `col_siklbsrsfa`, DROP `col_ynwgneexdq`;
ALTER TABLE `tb_wsxpqlbxhr` DROP `col_xlcidwqtlg`;
ALTER TABLE `tb_wsxpqlbxhr` DROP KEY `uk_cpzrxvccoo`;
ALTER TABLE `tb_wsxpqlbxhr` DROP KEY `col_koftcyrhcl`;
