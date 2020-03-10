CREATE TABLE `tb_fyextrfwrp` (
  `col_dkfamtasnd` bigint(86) unsigned DEFAULT '1',
  `col_yzwppisdla` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET latin1,
  `col_sowlpsmtck` tinyblob,
  `col_nnggkdrfej` mediumint NULL DEFAULT '1',
  UNIQUE `col_sowlpsmtck` (`col_sowlpsmtck`(1),`col_nnggkdrfej`)
) DEFAULT CHARSET=latin1;
RENAME TABLE `tb_fyextrfwrp` TO `tb_mdvjbvmscg`;
RENAME TABLE `tb_mdvjbvmscg` TO `tb_kzznkaskgu`;
RENAME TABLE `tb_kzznkaskgu` TO `tb_cvpyawneqw`;
ALTER TABLE `tb_cvpyawneqw` ADD (`col_klkrbtckkp` varchar(114) NULL, `col_yfpelqlubr` bigint(252) unsigned zerofill NULL);
ALTER TABLE `tb_cvpyawneqw` ADD `col_gxyxiafdwy` tinyblob;
ALTER TABLE `tb_cvpyawneqw` ADD COLUMN (`col_spugbpzyyf` double NOT NULL, `col_wbsqhinkwg` text);
ALTER TABLE `tb_cvpyawneqw` ADD COLUMN `col_npeyxvumaf` numeric(23) NULL AFTER `col_klkrbtckkp`;
ALTER TABLE `tb_cvpyawneqw` ADD COLUMN (`col_jrdtjutkna` int unsigned, `col_geplpeokit` smallint);
ALTER TABLE `tb_cvpyawneqw` ADD UNIQUE `uk_ameijmmzqx` (`col_sowlpsmtck`(21),`col_nnggkdrfej`);
ALTER TABLE `tb_cvpyawneqw` ADD UNIQUE (`col_gxyxiafdwy`(28),`col_spugbpzyyf`);
ALTER TABLE `tb_cvpyawneqw` ALTER `col_yzwppisdla` SET DEFAULT 'enum_or_set_0';
ALTER TABLE `tb_cvpyawneqw` CHANGE COLUMN `col_sowlpsmtck` `col_hjhuktoxjb` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8 NULL DEFAULT 'enum_or_set_0' FIRST;
ALTER TABLE `tb_cvpyawneqw` CHANGE COLUMN `col_yfpelqlubr` `col_gumhdzmptr` longblob AFTER `col_jrdtjutkna`;
ALTER TABLE `tb_cvpyawneqw` DROP COLUMN `col_npeyxvumaf`, DROP COLUMN `col_dkfamtasnd`;
ALTER TABLE `tb_cvpyawneqw` DROP COLUMN `col_spugbpzyyf`, DROP COLUMN `col_geplpeokit`;
ALTER TABLE `tb_cvpyawneqw` DROP `col_yzwppisdla`;
ALTER TABLE `tb_cvpyawneqw` DROP `col_klkrbtckkp`, DROP `col_hjhuktoxjb`;
ALTER TABLE `tb_cvpyawneqw` DROP `col_gumhdzmptr`, DROP `col_wbsqhinkwg`;
