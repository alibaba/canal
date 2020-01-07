CREATE TABLE `tb_baxubvrijb` (
  `col_kcwxoovscx` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8 DEFAULT 'enum_or_set_0',
  `col_fpfpelfmso` float(57,8) NULL,
  `col_dbtkzzfxoi` varbinary(22) NOT NULL,
  CONSTRAINT symb_hrwhbxnbts PRIMARY KEY (`col_dbtkzzfxoi`(10)),
  UNIQUE `col_dbtkzzfxoi` (`col_dbtkzzfxoi`(2)),
  UNIQUE KEY `col_fpfpelfmso` (`col_fpfpelfmso`,`col_dbtkzzfxoi`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `tb_ddzfshecwo` (
  `col_rryluiawnq` varchar(24) CHARACTER SET latin1 DEFAULT '',
  `col_bdlbhtissa` year(4) NULL,
  `col_kfawipagkn` tinyblob
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
RENAME TABLE `tb_ddzfshecwo` TO `tb_bcsetyvuul`, `tb_baxubvrijb` TO `tb_nxbyklxowd`;
RENAME TABLE `tb_bcsetyvuul` TO `tb_dlnhztodao`;
ALTER TABLE `tb_nxbyklxowd` ADD `col_zkfycwevvo` time;
ALTER TABLE `tb_nxbyklxowd` ADD COLUMN (`col_ememhbgtcx` bit NULL DEFAULT b'0', `col_zrrfkkosvo` char(203) CHARACTER SET utf8mb4);
ALTER TABLE `tb_nxbyklxowd` ADD (`col_fpafdojbdn` decimal(9,9) NULL, `col_juolcidyaj` datetime DEFAULT '2019-07-04 00:00:00');
ALTER TABLE `tb_nxbyklxowd` ADD `col_qhgfympzxa` longtext;
ALTER TABLE `tb_nxbyklxowd` ADD COLUMN (`col_tymlykugck` integer(83) zerofill, `col_awkvpagorc` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT 'enum_or_set_0');
ALTER TABLE `tb_nxbyklxowd` ADD COLUMN `col_crrxfxfdzs` datetime(0);
ALTER TABLE `tb_nxbyklxowd` ADD COLUMN (`col_cgdxdxesyd` decimal NOT NULL, `col_pjkofcyiht` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8 DEFAULT 'enum_or_set_0');
ALTER TABLE `tb_nxbyklxowd` ADD COLUMN `col_iiqainhxkc` smallint(81) zerofill NULL;
ALTER TABLE `tb_nxbyklxowd` ADD (`col_kbnueyzmpm` tinyblob, `col_zixjkfdybk` binary NOT NULL);
ALTER TABLE `tb_nxbyklxowd` CHARACTER SET = utf8mb4;
ALTER TABLE `tb_nxbyklxowd` ADD UNIQUE `col_cgdxdxesyd`(`col_cgdxdxesyd`,`col_iiqainhxkc`);
ALTER TABLE `tb_nxbyklxowd` ADD UNIQUE KEY `uk_regnlkocpv` (`col_zrrfkkosvo`(16),`col_fpafdojbdn`);
ALTER TABLE `tb_nxbyklxowd` ALTER `col_dbtkzzfxoi` DROP DEFAULT;
ALTER TABLE `tb_nxbyklxowd` ALTER `col_iiqainhxkc` DROP DEFAULT;
ALTER TABLE `tb_nxbyklxowd` ALTER `col_kcwxoovscx` DROP DEFAULT;
ALTER TABLE `tb_nxbyklxowd` CHANGE COLUMN `col_fpfpelfmso` `col_xkzfmyvfvv` smallint DEFAULT '1' FIRST;
ALTER TABLE `tb_nxbyklxowd` DROP `col_juolcidyaj`, DROP `col_pjkofcyiht`;
ALTER TABLE `tb_nxbyklxowd` DROP COLUMN `col_xkzfmyvfvv`;
ALTER TABLE `tb_nxbyklxowd` DROP INDEX `col_cgdxdxesyd`;
ALTER TABLE `tb_nxbyklxowd` DROP INDEX `uk_regnlkocpv`;
