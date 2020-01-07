CREATE TABLE `tb_rabcferkcm` (
  `col_fflqfgfdyt` varchar(26) CHARACTER SET utf8mb4 NOT NULL DEFAULT ''
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
RENAME TABLE `tb_rabcferkcm` TO `tb_ayfynftwmh`;
RENAME TABLE `tb_ayfynftwmh` TO `tb_eufiooviox`;
RENAME TABLE `tb_eufiooviox` TO `tb_txslclddib`;
ALTER TABLE `tb_txslclddib` ADD COLUMN (`col_vkgmvcluzx` bigint unsigned zerofill NULL, `col_zssozexdev` mediumtext CHARACTER SET utf8);
ALTER TABLE `tb_txslclddib` ADD `col_yvgymfxalr` year NULL DEFAULT '2019' AFTER `col_fflqfgfdyt`;
ALTER TABLE `tb_txslclddib` ADD (`col_qjmexurrhz` mediumblob, `col_gileulcldv` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') DEFAULT 'enum_or_set_0');
ALTER TABLE `tb_txslclddib` ADD COLUMN `col_lzgdrufqst` blob(2902581366) AFTER `col_gileulcldv`;
ALTER TABLE `tb_txslclddib` ADD COLUMN (`col_outygtpgyl` numeric(49) NULL, `col_wdxmmljumj` numeric(35));
ALTER TABLE `tb_txslclddib` ADD `col_whoyzvbsui` binary(117) FIRST;
ALTER TABLE `tb_txslclddib` ADD `col_pphxdgjxqs` timestamp;
ALTER TABLE `tb_txslclddib` ADD UNIQUE INDEX (`col_zssozexdev`(11),`col_qjmexurrhz`(14));
ALTER TABLE `tb_txslclddib` ADD UNIQUE INDEX `uk_uvxmxuhrsq` (`col_wdxmmljumj`,`col_pphxdgjxqs`);
ALTER TABLE `tb_txslclddib` ALTER `col_outygtpgyl` DROP DEFAULT;
ALTER TABLE `tb_txslclddib` CHANGE `col_qjmexurrhz` `col_wyitzglcmb` float NULL DEFAULT '1' AFTER `col_outygtpgyl`;
ALTER TABLE `tb_txslclddib` CHANGE `col_gileulcldv` `col_mnviktcwsx` blob;
ALTER TABLE `tb_txslclddib` CHANGE COLUMN `col_wyitzglcmb` `col_prvpzaqcny` integer zerofill;
ALTER TABLE `tb_txslclddib` DROP `col_wdxmmljumj`;
ALTER TABLE `tb_txslclddib` DROP COLUMN `col_pphxdgjxqs`, DROP COLUMN `col_lzgdrufqst`;
ALTER TABLE `tb_txslclddib` DROP `col_outygtpgyl`;
ALTER TABLE `tb_txslclddib` DROP `col_zssozexdev`;
ALTER TABLE `tb_txslclddib` DROP `col_whoyzvbsui`, DROP `col_prvpzaqcny`;
