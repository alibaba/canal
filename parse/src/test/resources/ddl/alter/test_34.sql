CREATE TABLE `tb_qlkjrjqnps` (
  `col_ktdqdryygn` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8 NOT NULL DEFAULT 'enum_or_set_0',
  `col_cbcrcayfoe` blob(3373722532)
) DEFAULT CHARSET=utf8;
CREATE TABLE `tb_whplqdohkv` LIKE `tb_qlkjrjqnps`;
CREATE TABLE `tb_ygcnpvqeez` LIKE `tb_whplqdohkv`;
RENAME TABLE `tb_whplqdohkv` TO `tb_jnhvwsafrh`;
ALTER TABLE `tb_qlkjrjqnps` ADD COLUMN `col_vkcmtobywn` binary NOT NULL;
ALTER TABLE `tb_qlkjrjqnps` ADD (`col_eblttrbrlq` varbinary(35) NULL DEFAULT '\0', `col_rygyddsstu` tinytext CHARACTER SET utf8mb4);
ALTER TABLE `tb_qlkjrjqnps` ADD `col_hkwgwwaxoy` text AFTER `col_eblttrbrlq`;
ALTER TABLE `tb_qlkjrjqnps` ADD COLUMN (`col_oilsudgyas` smallint(39) unsigned zerofill, `col_wgsnbmgdbz` int unsigned zerofill);
ALTER TABLE `tb_qlkjrjqnps` ADD COLUMN `col_opbtiidusc` tinyint zerofill NULL;
ALTER TABLE `tb_qlkjrjqnps` ADD (`col_ecaramctpw` varbinary(185) NOT NULL, `col_wcpdsgpcip` longblob);
ALTER TABLE `tb_qlkjrjqnps` ADD COLUMN (`col_fidmrllusm` varchar(132) CHARACTER SET utf8 NULL, `col_qxsokmevlv` integer zerofill NOT NULL);
ALTER TABLE `tb_qlkjrjqnps` ADD COLUMN `col_eoeroydtse` smallint(32) DEFAULT '1' FIRST;
ALTER TABLE `tb_qlkjrjqnps` ADD CONSTRAINT symb_rvaqwhjyyk PRIMARY KEY (`col_ktdqdryygn`,`col_vkcmtobywn`);
ALTER TABLE `tb_qlkjrjqnps` ALTER COLUMN `col_opbtiidusc` SET DEFAULT NULL;
ALTER TABLE `tb_qlkjrjqnps` ALTER `col_wgsnbmgdbz` DROP DEFAULT;
ALTER TABLE `tb_qlkjrjqnps` CHANGE COLUMN `col_eblttrbrlq` `col_mywjrvfxmw` tinyblob;
ALTER TABLE `tb_qlkjrjqnps` CHANGE COLUMN `col_rygyddsstu` `col_zuhebeqqrq` numeric(4,0);
ALTER TABLE `tb_qlkjrjqnps` DROP COLUMN `col_ecaramctpw`, DROP COLUMN `col_ktdqdryygn`;
