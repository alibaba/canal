CREATE TABLE `tb_aaxiqmpjjd` (
  `col_psfdpvpotp` integer(136) unsigned
) DEFAULT CHARSET=latin1;
CREATE TABLE `tb_axknhurptj` (
  `col_ezolqkylrw` bigint DEFAULT '1',
  `col_itrckjigax` tinyint zerofill,
  `col_vywhhgaozj` year,
  `col_xdrbsjifvv` datetime(1)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE TABLE `tb_ciqhkflcci` (
  `col_dgkncfpqja` bit(46) DEFAULT b'0',
  UNIQUE `col_dgkncfpqja` (`col_dgkncfpqja`),
  UNIQUE `uk_kcnspithfv` (`col_dgkncfpqja`)
) DEFAULT CHARSET=latin1;
RENAME TABLE `tb_axknhurptj` TO `tb_iilujirnjb`;
RENAME TABLE `tb_iilujirnjb` TO `tb_aftfwefuwe`;
DROP TABLE tb_aaxiqmpjjd;
DROP TABLE tb_ciqhkflcci, tb_aftfwefuwe;
CREATE TABLE `tb_ygyyvdctrs` (
  `col_emexlkeymz` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8mb4 DEFAULT 'enum_or_set_0',
  UNIQUE KEY `uk_kcmgrgarnq` (`col_emexlkeymz`),
  UNIQUE `col_emexlkeymz` (`col_emexlkeymz`)
) DEFAULT CHARSET=latin1;
ALTER TABLE `tb_ygyyvdctrs` ADD COLUMN `col_ljhqsygrcc` mediumtext FIRST;
ALTER TABLE `tb_ygyyvdctrs` ADD COLUMN (`col_ibmogxgtyp` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') DEFAULT 'enum_or_set_0', `col_lqozcajhii` tinyblob);
ALTER TABLE `tb_ygyyvdctrs` ADD (`col_yzbwkjwzqw` int zerofill NOT NULL, `col_dmdpojzsct` varbinary(166));
ALTER TABLE `tb_ygyyvdctrs` ADD `col_oxgbfbgzov` double(112,2);
ALTER TABLE `tb_ygyyvdctrs` ADD COLUMN `col_hirzxaomit` varbinary(145) NOT NULL FIRST;
ALTER TABLE `tb_ygyyvdctrs` ADD COLUMN `col_dxgsghrizm` tinyblob;
ALTER TABLE `tb_ygyyvdctrs` ADD `col_zkdwjtabnw` smallint unsigned NOT NULL AFTER `col_oxgbfbgzov`;
ALTER TABLE `tb_ygyyvdctrs` ADD COLUMN (`col_bowjbzwvyw` date, `col_inneappfsm` varchar(112) CHARACTER SET utf8mb4);
ALTER TABLE `tb_ygyyvdctrs` CHARACTER SET utf8;
ALTER TABLE `tb_ygyyvdctrs` ADD CONSTRAINT PRIMARY KEY (`col_hirzxaomit`(25));
ALTER TABLE `tb_ygyyvdctrs` ALTER COLUMN `col_ibmogxgtyp` DROP DEFAULT;
ALTER TABLE `tb_ygyyvdctrs` ALTER COLUMN `col_hirzxaomit` DROP DEFAULT;
ALTER TABLE `tb_ygyyvdctrs` ALTER COLUMN `col_ibmogxgtyp` SET DEFAULT 'enum_or_set_0';
ALTER TABLE `tb_ygyyvdctrs` CHANGE COLUMN `col_ljhqsygrcc` `col_rpxmbgbwos` longblob;
ALTER TABLE `tb_ygyyvdctrs` CHANGE `col_lqozcajhii` `col_qdbxvykcyv` tinytext CHARACTER SET utf8 COLLATE utf8_unicode_ci;
ALTER TABLE `tb_ygyyvdctrs` DROP COLUMN `col_qdbxvykcyv`;
ALTER TABLE `tb_ygyyvdctrs` DROP PRIMARY KEY;
ALTER TABLE `tb_ygyyvdctrs` DROP KEY `uk_kcmgrgarnq`;
