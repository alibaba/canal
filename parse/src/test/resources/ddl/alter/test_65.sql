CREATE TABLE `tb_vsqjzkimwi` (
  `col_dyxnwiudqo` mediumtext CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  `col_savmueldgi` blob(950926319),
  `col_gzatpknajd` float DEFAULT '1',
  `col_ebfnpjymeg` tinytext CHARACTER SET utf8mb4,
  UNIQUE `uk_iortgxjkuc` (`col_savmueldgi`(3))
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE TABLE `tb_pdtvascuyy` (
  `col_sbuommslma` timestamp(3) DEFAULT CURRENT_TIMESTAMP(3),
  `col_corovzewna` date NULL DEFAULT '2019-07-04',
  UNIQUE KEY `col_sbuommslma` (`col_sbuommslma`),
  UNIQUE INDEX `col_sbuommslma_2` (`col_sbuommslma`)
) DEFAULT CHARSET=utf8;
RENAME TABLE `tb_pdtvascuyy` TO `tb_rnradhkfon`, `tb_vsqjzkimwi` TO `tb_ambgylvzkp`;
ALTER TABLE `tb_ambgylvzkp` ADD (`col_dlbzhcqiqy` mediumint(77) unsigned zerofill, `col_siqozvxhgs` set('enum_or_set_0','enum_or_set_1','enum_or_set_2'));
ALTER TABLE `tb_ambgylvzkp` ADD COLUMN (`col_llpqomujks` varchar(24) NOT NULL DEFAULT '', `col_xkicftbwbg` mediumtext);
ALTER TABLE `tb_ambgylvzkp` ADD COLUMN (`col_dkjrphxpni` bit(4), `col_gbegrurwao` year(4) NOT NULL DEFAULT '2019');
ALTER TABLE `tb_ambgylvzkp` ADD COLUMN `col_xsgbuhbqwd` mediumblob AFTER `col_ebfnpjymeg`;
ALTER TABLE `tb_ambgylvzkp` ADD (`col_ubwwasrhlw` tinyblob, `col_uetuxzylgb` integer unsigned zerofill NOT NULL);
ALTER TABLE `tb_ambgylvzkp` ADD COLUMN (`col_bbyekxjypk` bigint(195) unsigned zerofill NULL, `col_eeieaubbdp` numeric(12));
ALTER TABLE `tb_ambgylvzkp` ADD COLUMN `col_smbnzspzed` date NULL AFTER `col_savmueldgi`;
ALTER TABLE `tb_ambgylvzkp` ADD COLUMN `col_nzhjdxjqxl` blob AFTER `col_dkjrphxpni`;
ALTER TABLE `tb_ambgylvzkp` ADD COLUMN (`col_dwnlzyhikn` char(158), `col_evvhlyienk` char(172) CHARACTER SET utf8mb4 NULL);
ALTER TABLE `tb_ambgylvzkp` CHARACTER SET utf8;
ALTER TABLE `tb_ambgylvzkp` ADD PRIMARY KEY (`col_llpqomujks`,`col_gbegrurwao`);
ALTER TABLE `tb_ambgylvzkp` ADD UNIQUE (`col_gbegrurwao`,`col_ubwwasrhlw`(19));
ALTER TABLE `tb_ambgylvzkp` ADD UNIQUE KEY `uk_mvcfpncfde` (`col_xkicftbwbg`(17),`col_gbegrurwao`);
ALTER TABLE `tb_ambgylvzkp` CHANGE COLUMN `col_uetuxzylgb` `col_lhhoxkkbgt` char FIRST;
ALTER TABLE `tb_ambgylvzkp` CHANGE COLUMN `col_gbegrurwao` `col_nkvjzenagy` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8mb4 NOT NULL;
ALTER TABLE `tb_ambgylvzkp` DROP COLUMN `col_dlbzhcqiqy`, DROP COLUMN `col_siqozvxhgs`;
ALTER TABLE `tb_ambgylvzkp` DROP COLUMN `col_nzhjdxjqxl`, DROP COLUMN `col_gzatpknajd`;
ALTER TABLE `tb_ambgylvzkp` DROP COLUMN `col_xsgbuhbqwd`;
ALTER TABLE `tb_ambgylvzkp` DROP `col_savmueldgi`, DROP `col_ubwwasrhlw`;
ALTER TABLE `tb_ambgylvzkp` DROP COLUMN `col_bbyekxjypk`;
ALTER TABLE `tb_ambgylvzkp` DROP COLUMN `col_dkjrphxpni`, DROP COLUMN `col_dyxnwiudqo`;
ALTER TABLE `tb_ambgylvzkp` DROP COLUMN `col_dwnlzyhikn`;
ALTER TABLE `tb_ambgylvzkp` DROP `col_nkvjzenagy`, DROP `col_lhhoxkkbgt`;
ALTER TABLE `tb_ambgylvzkp` DROP COLUMN `col_xkicftbwbg`;
