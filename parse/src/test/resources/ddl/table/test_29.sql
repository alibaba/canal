CREATE TABLE `tb_wczdgujkxe` (
  `col_mprviuzmsa` mediumint(215) unsigned zerofill,
  `col_wmnengkfur` decimal(6) NOT NULL,
  `col_toueuqhkhc` text(1378767894) CHARACTER SET utf8mb4,
  CONSTRAINT PRIMARY KEY (`col_wmnengkfur`),
  CONSTRAINT `symb_ytbnzrnwgs` UNIQUE (`col_toueuqhkhc`(31)),
  UNIQUE KEY `col_mprviuzmsa` (`col_mprviuzmsa`,`col_wmnengkfur`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `tb_cvcsopmfuk` LIKE `tb_wczdgujkxe`;
RENAME TABLE `tb_wczdgujkxe` TO `tb_alxysrcrzk`, `tb_cvcsopmfuk` TO `tb_bcgwxxtrfr`;
RENAME TABLE `tb_bcgwxxtrfr` TO `tb_qfvsxuvwcl`, `tb_alxysrcrzk` TO `tb_esuwpuxvcf`;
RENAME TABLE `tb_qfvsxuvwcl` TO `tb_nsqdmubznz`, `tb_esuwpuxvcf` TO `tb_rmphoinujw`;
RENAME TABLE `tb_nsqdmubznz` TO `tb_fgebibysbk`, `tb_rmphoinujw` TO `tb_xntcbmvmxc`;
RENAME TABLE `tb_xntcbmvmxc` TO `tb_jmydpaxtpu`, `tb_fgebibysbk` TO `tb_bkldmexfby`;
CREATE TABLE `tb_wczdgujkxe` (
  `col_mprviuzmsa` mediumint(215) unsigned zerofill,
  `col_wmnengkfur` decimal(6) NOT NULL,
  `col_toueuqhkhc` text(1378767894) CHARACTER SET utf8mb4,
  CONSTRAINT PRIMARY KEY (`col_wmnengkfur`),
  CONSTRAINT `symb_ytbnzrnwgs` UNIQUE (`col_toueuqhkhc`(31)),
  UNIQUE KEY `col_mprviuzmsa` (`col_mprviuzmsa`,`col_wmnengkfur`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `tb_cvcsopmfuk` LIKE `tb_wczdgujkxe`;
RENAME TABLE `tb_wczdgujkxe` TO `tb_alxysrcrzk`, `tb_cvcsopmfuk` TO `tb_bcgwxxtrfr`;
RENAME TABLE `tb_bcgwxxtrfr` TO `tb_qfvsxuvwcl`, `tb_alxysrcrzk` TO `tb_esuwpuxvcf`;
RENAME TABLE `tb_qfvsxuvwcl` TO `tb_nsqdmubznz`, `tb_esuwpuxvcf` TO `tb_rmphoinujw`;
RENAME TABLE `tb_nsqdmubznz` TO `tb_fgebibysbk`, `tb_rmphoinujw` TO `tb_xntcbmvmxc`;
RENAME TABLE `tb_xntcbmvmxc` TO `tb_jmydpaxtpu`, `tb_fgebibysbk` TO `tb_bkldmexfby`;
