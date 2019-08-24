CREATE TABLE `tb_pwgqnfglhb` (
  `col_iupsnmieyl` smallint unsigned NULL,
  `col_gnkgsewitj` numeric DEFAULT '1',
  `col_cvsykxnvpf` varchar(24) CHARACTER SET utf8 NULL DEFAULT '',
  `col_xailqzhuwy` blob
) DEFAULT CHARSET=utf8;
CREATE TABLE `tb_zhhkhdmeqy` LIKE `tb_pwgqnfglhb`;
RENAME TABLE `tb_pwgqnfglhb` TO `tb_igmuoxapjx`, `tb_zhhkhdmeqy` TO `tb_wmwynbzgyg`;
ALTER TABLE `tb_igmuoxapjx` DROP `col_xailqzhuwy`;
ALTER TABLE `tb_igmuoxapjx` DROP COLUMN `col_gnkgsewitj`, DROP COLUMN `col_iupsnmieyl`;
