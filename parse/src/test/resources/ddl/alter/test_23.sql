CREATE TABLE `tb_caahynuejc` (
  `col_ceosnfriqs` integer,
  `col_aklqoixrun` year(4),
  UNIQUE INDEX `col_ceosnfriqs` (`col_ceosnfriqs`),
  UNIQUE `uk_gekdmzrybz` (`col_aklqoixrun`)
) DEFAULT CHARSET=utf8;
CREATE TABLE `tb_xbsrcfksox` (
  `col_qsljaaxbrj` datetime DEFAULT '2019-07-04 00:00:00',
  `col_vusitjjcdx` char CHARACTER SET latin1,
  `col_ufwmvwjonj` integer unsigned NOT NULL,
  UNIQUE KEY `uk_henpwhqovc` (`col_ufwmvwjonj`),
  UNIQUE INDEX `col_vusitjjcdx` (`col_vusitjjcdx`,`col_ufwmvwjonj`)
) DEFAULT CHARSET=latin1;
CREATE TABLE `tb_eroteuzada` (
  `col_aipblmdcsw` float(32) NOT NULL,
  `col_sklhosmnvr` bigint unsigned zerofill NOT NULL,
  `col_skwdbrxmlc` mediumblob,
  UNIQUE INDEX `col_aipblmdcsw` (`col_aipblmdcsw`,`col_sklhosmnvr`),
  UNIQUE INDEX `uk_inmzvqrjoq` (`col_sklhosmnvr`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
RENAME TABLE `tb_eroteuzada` TO `tb_thlwjjfpcg`;
RENAME TABLE `tb_thlwjjfpcg` TO `tb_ydbfvvwhvi`;
ALTER TABLE `tb_caahynuejc` ADD COLUMN `col_noipbutgvd` char CHARACTER SET utf8mb4 NULL;
ALTER TABLE `tb_caahynuejc` ADD (`col_eyefseknmn` time DEFAULT '00:00:00', `col_nxbdoexmux` bigint(139) zerofill NULL);
ALTER TABLE `tb_caahynuejc` ADD `col_aulqirvfec` tinyblob;
ALTER TABLE `tb_caahynuejc` ADD COLUMN (`col_tlcpasbobp` bigint zerofill NULL, `col_fyktzflvgx` tinytext CHARACTER SET utf8);
ALTER TABLE `tb_caahynuejc` ADD (`col_bufdgcwxci` time DEFAULT '00:00:00', `col_snfugvbwkh` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8 NULL);
ALTER TABLE `tb_caahynuejc` ADD (`col_hcvfpfsxxl` longblob, `col_tnpnwxtkvp` longblob);
ALTER TABLE `tb_caahynuejc` DEFAULT CHARACTER SET utf8;
ALTER TABLE `tb_caahynuejc` ADD UNIQUE KEY `uk_kvrvlewpkc` (`col_bufdgcwxci`);
ALTER TABLE `tb_caahynuejc` ALTER `col_eyefseknmn` DROP DEFAULT;
ALTER TABLE `tb_caahynuejc` CHANGE `col_fyktzflvgx` `col_dijbniwgme` decimal(20,1) NULL;
ALTER TABLE `tb_caahynuejc` CHANGE `col_aulqirvfec` `col_wpzatdhhyw` datetime NOT NULL DEFAULT '2019-07-04 00:00:00';
ALTER TABLE `tb_caahynuejc` DROP COLUMN `col_bufdgcwxci`;
ALTER TABLE `tb_caahynuejc` DROP COLUMN `col_snfugvbwkh`;
ALTER TABLE `tb_caahynuejc` DROP COLUMN `col_tnpnwxtkvp`;
ALTER TABLE `tb_caahynuejc` DROP `col_nxbdoexmux`, DROP `col_hcvfpfsxxl`;
ALTER TABLE `tb_caahynuejc` DROP COLUMN `col_dijbniwgme`;
ALTER TABLE `tb_caahynuejc` DROP COLUMN `col_wpzatdhhyw`;
