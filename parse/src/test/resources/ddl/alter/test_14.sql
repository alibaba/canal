CREATE TABLE `tb_mzhjgiljck` (
  `col_lnqceyzqyi` mediumint DEFAULT '1',
  `col_rorzwqbqzc` varbinary(190) NULL,
  `col_qfegkgeaal` tinyint unsigned DEFAULT '1',
  UNIQUE `uk_nvoltofkla` (`col_rorzwqbqzc`(15))
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `tb_jwpndmdumd` (
  `col_vpupwhoacr` bit(52) DEFAULT b'0',
  UNIQUE `uk_ghdynafkbb` (`col_vpupwhoacr`),
  UNIQUE `uk_pdhqnqgdnh` (`col_vpupwhoacr`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
RENAME TABLE `tb_jwpndmdumd` TO `tb_xnqicybdlo`, `tb_mzhjgiljck` TO `tb_cmzqttalvi`;
ALTER TABLE `tb_xnqicybdlo` ADD COLUMN `col_jzysxyqbvv` binary(91) NULL;
ALTER TABLE `tb_xnqicybdlo` ADD `col_ldvvuuqjus` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') DEFAULT 'enum_or_set_0' FIRST;
ALTER TABLE `tb_xnqicybdlo` ADD (`col_drciitekzb` longblob, `col_ocncfebity` numeric(32,27));
ALTER TABLE `tb_xnqicybdlo` ADD `col_uzoealeegi` binary;
ALTER TABLE `tb_xnqicybdlo` ADD COLUMN (`col_qnchccpalx` binary(157) NULL, `col_daawnfezqe` datetime(1) NULL);
ALTER TABLE `tb_xnqicybdlo` ADD UNIQUE INDEX (`col_jzysxyqbvv`(31),`col_uzoealeegi`);
ALTER TABLE `tb_xnqicybdlo` ADD UNIQUE `uk_spjmyismss` (`col_ldvvuuqjus`);
ALTER TABLE `tb_xnqicybdlo` ALTER COLUMN `col_uzoealeegi` SET DEFAULT NULL;
ALTER TABLE `tb_xnqicybdlo` ALTER COLUMN `col_jzysxyqbvv` DROP DEFAULT;
ALTER TABLE `tb_xnqicybdlo` ALTER COLUMN `col_qnchccpalx` DROP DEFAULT;
ALTER TABLE `tb_xnqicybdlo` DROP `col_vpupwhoacr`;
ALTER TABLE `tb_xnqicybdlo` DROP COLUMN `col_daawnfezqe`, DROP COLUMN `col_uzoealeegi`;
ALTER TABLE `tb_xnqicybdlo` DROP `col_ocncfebity`, DROP `col_ldvvuuqjus`;
ALTER TABLE `tb_xnqicybdlo` DROP `col_qnchccpalx`;
ALTER TABLE `tb_xnqicybdlo` DROP `col_drciitekzb`;
