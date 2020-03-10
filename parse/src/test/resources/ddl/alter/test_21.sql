CREATE TABLE `tb_kcqprbgueu` (
  `col_bkngorvqcs` blob(236574804),
  `col_zzurkpmmun` year(4) DEFAULT '2019',
  `col_ysfndnoaxw` varchar(53) CHARACTER SET utf8 NOT NULL DEFAULT '',
  PRIMARY KEY (`col_ysfndnoaxw`(25)),
  UNIQUE `col_bkngorvqcs` (`col_bkngorvqcs`(26),`col_zzurkpmmun`),
  UNIQUE `uk_gjdewbtjjb` (`col_zzurkpmmun`)
) DEFAULT CHARSET=utf8;
RENAME TABLE `tb_kcqprbgueu` TO `tb_vsurmlbmno`;
RENAME TABLE `tb_vsurmlbmno` TO `tb_aaholafery`;
RENAME TABLE `tb_aaholafery` TO `tb_mjvfhaqskb`;
ALTER TABLE `tb_mjvfhaqskb` ADD `col_njoaqbbtxv` bit(26) DEFAULT b'0';
ALTER TABLE `tb_mjvfhaqskb` ADD COLUMN `col_mipvhqbjat` mediumtext CHARACTER SET utf8 AFTER `col_zzurkpmmun`;
ALTER TABLE `tb_mjvfhaqskb` ADD COLUMN `col_bjzwkjcmer` decimal(17,10) FIRST;
ALTER TABLE `tb_mjvfhaqskb` ADD COLUMN (`col_qnkieomtxh` datetime(5), `col_olvjbrfhvy` char NULL);
ALTER TABLE `tb_mjvfhaqskb` ADD `col_bnupaoxxfv` blob(1570619016) AFTER `col_qnkieomtxh`;
ALTER TABLE `tb_mjvfhaqskb` ADD UNIQUE `uk_sidczmsazr` (`col_bkngorvqcs`(6),`col_mipvhqbjat`(25));
ALTER TABLE `tb_mjvfhaqskb` ALTER COLUMN `col_zzurkpmmun` DROP DEFAULT;
ALTER TABLE `tb_mjvfhaqskb` CHANGE COLUMN `col_bkngorvqcs` `col_kxxtstiztr` mediumblob;
ALTER TABLE `tb_mjvfhaqskb` CHANGE COLUMN `col_njoaqbbtxv` `col_snmdnyljqk` varbinary(47) DEFAULT '\0' FIRST;
ALTER TABLE `tb_mjvfhaqskb` CHANGE `col_kxxtstiztr` `col_bfmjyipktn` mediumtext FIRST;
ALTER TABLE `tb_mjvfhaqskb` DROP `col_bfmjyipktn`;
ALTER TABLE `tb_mjvfhaqskb` DROP COLUMN `col_zzurkpmmun`, DROP COLUMN `col_ysfndnoaxw`;
ALTER TABLE `tb_mjvfhaqskb` DROP COLUMN `col_qnkieomtxh`;
ALTER TABLE `tb_mjvfhaqskb` DROP COLUMN `col_bjzwkjcmer`;
ALTER TABLE `tb_mjvfhaqskb` DROP `col_mipvhqbjat`, DROP `col_olvjbrfhvy`;
ALTER TABLE `tb_mjvfhaqskb` DROP `col_bnupaoxxfv`;
