CREATE TABLE `tb_teqnorpcun` (
  `col_ufqdyzbxyc` longtext CHARACTER SET utf8mb4,
  UNIQUE KEY `col_ufqdyzbxyc` (`col_ufqdyzbxyc`(19)),
  UNIQUE KEY `col_ufqdyzbxyc_2` (`col_ufqdyzbxyc`(18))
) DEFAULT CHARSET=utf8;
RENAME TABLE `tb_teqnorpcun` TO `tb_plmerlrvqm`;
RENAME TABLE `tb_plmerlrvqm` TO `tb_putsugoooc`;
ALTER TABLE `tb_putsugoooc` ADD `col_eykecckmna` float(240,27) FIRST;
ALTER TABLE `tb_putsugoooc` ADD `col_anwqmnbtnq` char CHARACTER SET utf8 FIRST;
ALTER TABLE `tb_putsugoooc` ADD `col_zqtvmwyqye` tinyblob AFTER `col_ufqdyzbxyc`;
ALTER TABLE `tb_putsugoooc` ADD (`col_ptbaaugrfk` timestamp NULL, `col_nzheqbovze` longtext CHARACTER SET utf8mb4);
ALTER TABLE `tb_putsugoooc` ADD COLUMN `col_ewavlcwcvf` bigint(210) unsigned zerofill NOT NULL FIRST;
ALTER TABLE `tb_putsugoooc` ADD (`col_kxxvdiusoj` tinyblob, `col_gkjiivrixs` double(212,20) NULL);
ALTER TABLE `tb_putsugoooc` CHARACTER SET utf8mb4;
ALTER TABLE `tb_putsugoooc` ADD PRIMARY KEY (`col_ewavlcwcvf`);
ALTER TABLE `tb_putsugoooc` CHANGE COLUMN `col_nzheqbovze` `col_hwjcbrppgw` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8 DEFAULT 'enum_or_set_0' FIRST;
ALTER TABLE `tb_putsugoooc` CHANGE `col_ufqdyzbxyc` `col_ynpynwnkjv` mediumtext CHARACTER SET utf8mb4 FIRST;
ALTER TABLE `tb_putsugoooc` DROP `col_anwqmnbtnq`;
ALTER TABLE `tb_putsugoooc` DROP `col_gkjiivrixs`, DROP `col_kxxvdiusoj`;
ALTER TABLE `tb_putsugoooc` DROP COLUMN `col_zqtvmwyqye`;
ALTER TABLE `tb_putsugoooc` DROP `col_hwjcbrppgw`, DROP `col_ptbaaugrfk`;
ALTER TABLE `tb_putsugoooc` DROP `col_eykecckmna`;
ALTER TABLE `tb_putsugoooc` DROP COLUMN `col_ewavlcwcvf`;
ALTER TABLE `tb_putsugoooc` DROP INDEX `col_ufqdyzbxyc`;
