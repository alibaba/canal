CREATE TABLE `tb_ofxsxmoixc` (
  `col_tymnumzsjd` tinytext CHARACTER SET latin1,
  `col_zvcpajjdrr` integer(26) zerofill,
  UNIQUE `col_tymnumzsjd` (`col_tymnumzsjd`(2))
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
RENAME TABLE `tb_ofxsxmoixc` TO `tb_mriknxonls`;
RENAME TABLE `tb_mriknxonls` TO `tb_hkehwyglax`;
ALTER TABLE `tb_hkehwyglax` ADD `col_hpedrlkbit` tinyblob FIRST;
ALTER TABLE `tb_hkehwyglax` ADD (`col_aftchdhhpb` integer unsigned NULL, `col_engwamylfv` text);
ALTER TABLE `tb_hkehwyglax` ADD `col_dcnadmyyfb` binary FIRST;
ALTER TABLE `tb_hkehwyglax` ADD COLUMN (`col_epoaxrbmcm` double NOT NULL, `col_qknitkqdnz` longblob);
ALTER TABLE `tb_hkehwyglax` ADD COLUMN `col_upulxkgdxc` bigint;
ALTER TABLE `tb_hkehwyglax` ADD COLUMN (`col_fdyhioyktq` tinytext CHARACTER SET utf8mb4, `col_szfqsvrruq` smallint zerofill NULL);
ALTER TABLE `tb_hkehwyglax` ADD (`col_iqohbjwihd` text CHARACTER SET utf8mb4, `col_yxsvpkijew` float(43) NOT NULL);
ALTER TABLE `tb_hkehwyglax` ADD CONSTRAINT PRIMARY KEY (`col_epoaxrbmcm`);
ALTER TABLE `tb_hkehwyglax` ADD UNIQUE KEY `uk_tkjaqutxko` (`col_yxsvpkijew`);
ALTER TABLE `tb_hkehwyglax` ADD UNIQUE (`col_tymnumzsjd`(17),`col_aftchdhhpb`);
ALTER TABLE `tb_hkehwyglax` CHANGE `col_engwamylfv` `col_ivaaxawlsl` mediumtext CHARACTER SET utf8mb4 AFTER `col_hpedrlkbit`;
ALTER TABLE `tb_hkehwyglax` CHANGE COLUMN `col_qknitkqdnz` `col_puxtweqbpn` mediumblob AFTER `col_dcnadmyyfb`;
ALTER TABLE `tb_hkehwyglax` DROP COLUMN `col_epoaxrbmcm`;
ALTER TABLE `tb_hkehwyglax` DROP `col_szfqsvrruq`;
ALTER TABLE `tb_hkehwyglax` DROP COLUMN `col_tymnumzsjd`;
ALTER TABLE `tb_hkehwyglax` DROP `col_hpedrlkbit`, DROP `col_puxtweqbpn`;
ALTER TABLE `tb_hkehwyglax` DROP `col_ivaaxawlsl`;
ALTER TABLE `tb_hkehwyglax` DROP COLUMN `col_upulxkgdxc`;
ALTER TABLE `tb_hkehwyglax` DROP KEY `uk_tkjaqutxko`;
