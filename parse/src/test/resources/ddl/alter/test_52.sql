CREATE TABLE `tb_udrucjftxw` (
  `col_tustciiwwp` datetime(2),
  `col_xhjeuttrui` text(4218326145) CHARACTER SET utf8mb4,
  `col_gzwherugvm` decimal(35,10),
  `col_fwooxfmvvs` tinytext CHARACTER SET utf8,
  UNIQUE `uk_kllnpcqqzt` (`col_xhjeuttrui`(30)),
  UNIQUE INDEX `uk_omzcuexdxr` (`col_xhjeuttrui`(8),`col_gzwherugvm`)
) DEFAULT CHARSET=utf8;
CREATE TABLE `tb_ykcdymjhfs` LIKE `tb_udrucjftxw`;
RENAME TABLE `tb_ykcdymjhfs` TO `tb_dkkimqxscg`;
RENAME TABLE `tb_dkkimqxscg` TO `tb_syxeibzqzx`;
ALTER TABLE `tb_syxeibzqzx` ADD COLUMN (`col_orgcdijcqm` numeric(50,5) NULL, `col_shntqukbsk` datetime NULL);
ALTER TABLE `tb_syxeibzqzx` ADD COLUMN `col_ecxangkhqi` numeric DEFAULT '1' AFTER `col_gzwherugvm`;
ALTER TABLE `tb_syxeibzqzx` ADD `col_nekpdybkhy` integer(53) unsigned DEFAULT '1';
ALTER TABLE `tb_syxeibzqzx` ADD COLUMN (`col_sxwiebuapp` tinyint unsigned NULL DEFAULT '1', `col_mqllkiyvhv` bit DEFAULT b'0');
ALTER TABLE `tb_syxeibzqzx` ADD (`col_wfgcfbwoia` tinyint unsigned zerofill NULL, `col_ftvwzcealb` int DEFAULT '1');
ALTER TABLE `tb_syxeibzqzx` ADD UNIQUE INDEX `uk_puugjtgkjy` (`col_wfgcfbwoia`,`col_ftvwzcealb`);
ALTER TABLE `tb_syxeibzqzx` CHANGE `col_ftvwzcealb` `col_hvglzyyxbw` bigint unsigned NULL DEFAULT '1';
ALTER TABLE `tb_syxeibzqzx` CHANGE `col_orgcdijcqm` `col_gzrngpftzg` tinyint(238) NULL DEFAULT '1';
ALTER TABLE `tb_syxeibzqzx` CHANGE `col_tustciiwwp` `col_aqtxegfokt` tinyblob FIRST;
ALTER TABLE `tb_syxeibzqzx` DROP `col_sxwiebuapp`;
ALTER TABLE `tb_syxeibzqzx` DROP COLUMN `col_ecxangkhqi`, DROP COLUMN `col_wfgcfbwoia`;
ALTER TABLE `tb_syxeibzqzx` DROP COLUMN `col_gzrngpftzg`, DROP COLUMN `col_gzwherugvm`;
ALTER TABLE `tb_syxeibzqzx` DROP COLUMN `col_hvglzyyxbw`;
ALTER TABLE `tb_syxeibzqzx` DROP `col_nekpdybkhy`, DROP `col_aqtxegfokt`;
ALTER TABLE `tb_syxeibzqzx` DROP `col_xhjeuttrui`;
ALTER TABLE `tb_syxeibzqzx` DROP COLUMN `col_mqllkiyvhv`, DROP COLUMN `col_fwooxfmvvs`;
