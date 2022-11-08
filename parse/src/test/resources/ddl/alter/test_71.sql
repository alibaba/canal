CREATE TABLE `tb_wviibzdwnl` (
  `col_aslaluhepd` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8 DEFAULT 'enum_or_set_0',
  `col_aqelxtcidu` numeric NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
RENAME TABLE `tb_wviibzdwnl` TO `tb_sfwkwlflbi`;
ALTER TABLE `tb_sfwkwlflbi` ADD COLUMN `col_zblksizcse` blob AFTER `col_aqelxtcidu`;
ALTER TABLE `tb_sfwkwlflbi` ADD COLUMN (`col_ezxxxqpwon` time(2) NULL, `col_wryzwhfdap` mediumtext CHARACTER SET utf8 COLLATE utf8_unicode_ci);
ALTER TABLE `tb_sfwkwlflbi` DEFAULT CHARACTER SET utf8;
ALTER TABLE `tb_sfwkwlflbi` ADD UNIQUE KEY `uk_odmbvgzhkr` (`col_zblksizcse`(18),`col_ezxxxqpwon`);
ALTER TABLE `tb_sfwkwlflbi` CHANGE COLUMN `col_aqelxtcidu` `col_scvucmvmtf` float(38) FIRST;
ALTER TABLE `tb_sfwkwlflbi` CHANGE `col_ezxxxqpwon` `col_xvqalfjvnx` decimal DEFAULT '1' FIRST;
ALTER TABLE `tb_sfwkwlflbi` DROP COLUMN `col_aslaluhepd`;
ALTER TABLE `tb_sfwkwlflbi` DROP COLUMN `col_zblksizcse`, DROP COLUMN `col_scvucmvmtf`;
ALTER TABLE `tb_sfwkwlflbi` DROP COLUMN `col_xvqalfjvnx`;
