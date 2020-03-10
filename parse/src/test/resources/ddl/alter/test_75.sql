CREATE TABLE `tb_wumnowgbgm` (
  `col_dxnkzlgbrh` text(3378091990) CHARACTER SET latin1,
  `col_dbfjlhoypk` bit(27) NOT NULL,
  `col_wzrdexqlrw` smallint unsigned NOT NULL,
  `col_adfenvntyq` char CHARACTER SET utf8mb4,
  CONSTRAINT symb_jmvitdlrbv PRIMARY KEY (`col_dbfjlhoypk`,`col_wzrdexqlrw`),
  UNIQUE `uk_xoqexlvgan` (`col_adfenvntyq`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE TABLE `tb_xwnddbnhpl` LIKE `tb_wumnowgbgm`;
CREATE TABLE `tb_efrfggoljv` (
  `col_shpcvotegh` numeric(34),
  `col_watvncqjsq` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8 NULL DEFAULT 'enum_or_set_0',
  `col_zycpbpgevt` longtext CHARACTER SET latin1,
  UNIQUE `uk_jkvqqmmqwa` (`col_shpcvotegh`,`col_watvncqjsq`),
  UNIQUE INDEX `col_zycpbpgevt` (`col_zycpbpgevt`(22))
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
RENAME TABLE `tb_efrfggoljv` TO `tb_lznvphwjai`, `tb_wumnowgbgm` TO `tb_vinehxgnxe`;
DROP TABLE tb_vinehxgnxe;
DROP TABLE tb_lznvphwjai, tb_xwnddbnhpl;
CREATE TABLE `tb_mpjfjmkbwn` (
  `col_lyrrpdesag` bigint zerofill NULL,
  `col_fehlmkpxfq` char CHARACTER SET latin1,
  `col_sotvvevbho` year NULL DEFAULT '2019',
  UNIQUE KEY `uk_odozsruufn` (`col_fehlmkpxfq`),
  UNIQUE `col_lyrrpdesag` (`col_lyrrpdesag`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
ALTER TABLE `tb_mpjfjmkbwn` ADD `col_eyzpohpkcl` integer zerofill NOT NULL AFTER `col_lyrrpdesag`;
ALTER TABLE `tb_mpjfjmkbwn` ADD (`col_kfcxjyhzkq` varchar(178), `col_khvbberyat` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') NOT NULL);
ALTER TABLE `tb_mpjfjmkbwn` ADD COLUMN `col_ximpbczbvl` longblob FIRST;
ALTER TABLE `tb_mpjfjmkbwn` DEFAULT CHARACTER SET = utf8mb4;
ALTER TABLE `tb_mpjfjmkbwn` CHANGE `col_kfcxjyhzkq` `col_bpzkjdwbex` char CHARACTER SET utf8 COLLATE utf8_unicode_ci AFTER `col_fehlmkpxfq`;
ALTER TABLE `tb_mpjfjmkbwn` CHANGE `col_sotvvevbho` `col_whykzkssvd` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') NULL DEFAULT 'enum_or_set_0';
ALTER TABLE `tb_mpjfjmkbwn` CHANGE `col_bpzkjdwbex` `col_oqfdewvoin` bigint unsigned;
ALTER TABLE `tb_mpjfjmkbwn` DROP `col_khvbberyat`;
ALTER TABLE `tb_mpjfjmkbwn` DROP COLUMN `col_whykzkssvd`;
ALTER TABLE `tb_mpjfjmkbwn` DROP `col_ximpbczbvl`;
