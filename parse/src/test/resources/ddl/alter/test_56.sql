CREATE TABLE `tb_lvzjcjnwfj` (
  `col_wfdgvtipop` binary(1) NOT NULL,
  `col_dhvowgmope` date,
  `col_ztgybltpcr` bit(42) DEFAULT b'0',
  `col_rfeddmkkmb` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8mb4 NULL DEFAULT 'enum_or_set_0',
  CONSTRAINT symb_puisvgsxvj PRIMARY KEY (`col_wfdgvtipop`),
  UNIQUE `col_rfeddmkkmb` (`col_rfeddmkkmb`),
  UNIQUE INDEX `col_wfdgvtipop` (`col_wfdgvtipop`)
) DEFAULT CHARSET=utf8;
RENAME TABLE `tb_lvzjcjnwfj` TO `tb_gwhtsxuovt`;
RENAME TABLE `tb_gwhtsxuovt` TO `tb_gvuiytzqsi`;
RENAME TABLE `tb_gvuiytzqsi` TO `tb_hjqdntirzd`;
ALTER TABLE `tb_hjqdntirzd` ADD COLUMN `col_oirceeozit` mediumtext AFTER `col_wfdgvtipop`;
ALTER TABLE `tb_hjqdntirzd` ADD COLUMN (`col_ckheruciet` blob(829823439), `col_mwlpdcpegm` char NULL);
ALTER TABLE `tb_hjqdntirzd` ADD COLUMN (`col_okjyevogvl` integer NULL DEFAULT '1', `col_nperdtdonu` int(55) unsigned zerofill NOT NULL);
ALTER TABLE `tb_hjqdntirzd` ADD COLUMN (`col_cawkxhaucp` tinyblob, `col_urbzxvvjju` binary(3) NULL);
ALTER TABLE `tb_hjqdntirzd` CHARACTER SET = utf8mb4;
ALTER TABLE `tb_hjqdntirzd` ADD UNIQUE KEY `uk_hrnnhxlode` (`col_urbzxvvjju`);
ALTER TABLE `tb_hjqdntirzd` ADD UNIQUE (`col_mwlpdcpegm`,`col_nperdtdonu`);
ALTER TABLE `tb_hjqdntirzd` ALTER `col_nperdtdonu` DROP DEFAULT;
ALTER TABLE `tb_hjqdntirzd` CHANGE COLUMN `col_mwlpdcpegm` `col_dtiogubkiw` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8mb4 NULL DEFAULT 'enum_or_set_0' AFTER `col_ckheruciet`;
ALTER TABLE `tb_hjqdntirzd` CHANGE `col_nperdtdonu` `col_uwucrgteir` tinyint(120) DEFAULT '1';
ALTER TABLE `tb_hjqdntirzd` CHANGE `col_ztgybltpcr` `col_egpjhiepzh` smallint(0) DEFAULT '1';
ALTER TABLE `tb_hjqdntirzd` DROP `col_dhvowgmope`, DROP `col_wfdgvtipop`;
ALTER TABLE `tb_hjqdntirzd` DROP COLUMN `col_urbzxvvjju`, DROP COLUMN `col_rfeddmkkmb`;
ALTER TABLE `tb_hjqdntirzd` DROP `col_oirceeozit`, DROP `col_egpjhiepzh`;
ALTER TABLE `tb_hjqdntirzd` DROP COLUMN `col_uwucrgteir`, DROP COLUMN `col_ckheruciet`;
