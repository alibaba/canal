CREATE TABLE `tb_tmotlxzrja` (
  `col_uesfklwmsk` date DEFAULT '2019-07-04',
  `col_zskjzqcxkk` varbinary(218) DEFAULT '\0',
  `col_rvztkthbca` time(6) NULL,
  `col_mpmkktdhop` mediumblob,
  UNIQUE `col_rvztkthbca` (`col_rvztkthbca`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `tb_plneggfdkn` (
  `col_zzhxpptdgq` longblob,
  UNIQUE INDEX `uk_voojzaxmwk` (`col_zzhxpptdgq`(21))
) DEFAULT CHARSET=latin1;
RENAME TABLE `tb_plneggfdkn` TO `tb_ctvykdvyzx`;
RENAME TABLE `tb_ctvykdvyzx` TO `tb_amkojtlotu`;
ALTER TABLE `tb_tmotlxzrja` ADD COLUMN `col_kqarzddnnt` date;
ALTER TABLE `tb_tmotlxzrja` ADD `col_tgjhdvpopd` year;
ALTER TABLE `tb_tmotlxzrja` ADD COLUMN (`col_buqqlqfgsv` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') NULL DEFAULT 'enum_or_set_0', `col_erywvwybme` int(13) NULL DEFAULT '1');
ALTER TABLE `tb_tmotlxzrja` ADD COLUMN (`col_glrqptmjgp` varbinary(172), `col_lxemzphwpr` tinyblob);
ALTER TABLE `tb_tmotlxzrja` ADD COLUMN `col_nytimponye` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') NOT NULL;
ALTER TABLE `tb_tmotlxzrja` ADD `col_dmorocleic` numeric NULL;
ALTER TABLE `tb_tmotlxzrja` ADD COLUMN (`col_soiyoovkol` char CHARACTER SET utf8mb4 NULL, `col_kpuqcxwuvq` char NOT NULL);
ALTER TABLE `tb_tmotlxzrja` ADD `col_wppxtgrjdz` tinyblob;
ALTER TABLE `tb_tmotlxzrja` ALTER COLUMN `col_uesfklwmsk` SET DEFAULT '2019-07-04';
ALTER TABLE `tb_tmotlxzrja` DROP COLUMN `col_kpuqcxwuvq`;
ALTER TABLE `tb_tmotlxzrja` DROP `col_lxemzphwpr`;
ALTER TABLE `tb_tmotlxzrja` DROP `col_dmorocleic`;
ALTER TABLE `tb_tmotlxzrja` DROP COLUMN `col_nytimponye`;
ALTER TABLE `tb_tmotlxzrja` DROP `col_buqqlqfgsv`, DROP `col_kqarzddnnt`;
ALTER TABLE `tb_tmotlxzrja` DROP KEY `col_rvztkthbca`;
