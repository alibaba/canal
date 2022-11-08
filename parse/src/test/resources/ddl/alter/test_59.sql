CREATE TABLE `tb_dvddpenkxa` (
  `col_zswpqbctyi` tinyblob,
  `col_jdidaolbpa` tinyblob,
  `col_eemywvvaqc` float DEFAULT '1'
) DEFAULT CHARSET=utf8;
CREATE TABLE `tb_iccbmqajux` (
  `col_rtzczeoxsl` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8mb4 NULL DEFAULT 'enum_or_set_0',
  UNIQUE `col_rtzczeoxsl` (`col_rtzczeoxsl`),
  UNIQUE `uk_qbqnttuesx` (`col_rtzczeoxsl`)
) DEFAULT CHARSET=utf8;
RENAME TABLE `tb_dvddpenkxa` TO `tb_gqtjkbvgka`, `tb_iccbmqajux` TO `tb_sjauultjmo`;
RENAME TABLE `tb_sjauultjmo` TO `tb_kwnuzmzxqv`, `tb_gqtjkbvgka` TO `tb_aolqhhluko`;
RENAME TABLE `tb_aolqhhluko` TO `tb_pengxwurit`;
DROP TABLE tb_kwnuzmzxqv, tb_pengxwurit;
CREATE TABLE `tb_hbheuprskb` (
  `col_fdqpihurmr` time(0),
  `col_owakmhospa` longtext CHARACTER SET utf8mb4,
  `col_qdrqmqehyb` varbinary(89) NOT NULL,
  `col_mmxaoivplw` time NOT NULL DEFAULT '00:00:00',
  PRIMARY KEY (`col_qdrqmqehyb`(1)),
  UNIQUE INDEX `uk_fwmxmptctu` (`col_owakmhospa`(30),`col_mmxaoivplw`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
ALTER TABLE `tb_hbheuprskb` ADD COLUMN (`col_lxqjgyhhxu` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT 'enum_or_set_0', `col_mqqxkezdux` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') NOT NULL);
ALTER TABLE `tb_hbheuprskb` DEFAULT CHARACTER SET utf8mb4;
ALTER TABLE `tb_hbheuprskb` ADD UNIQUE INDEX (`col_mqqxkezdux`);
ALTER TABLE `tb_hbheuprskb` ALTER COLUMN `col_mmxaoivplw` SET DEFAULT '00:00:00';
ALTER TABLE `tb_hbheuprskb` ALTER COLUMN `col_mmxaoivplw` DROP DEFAULT;
ALTER TABLE `tb_hbheuprskb` ALTER COLUMN `col_mmxaoivplw` DROP DEFAULT;
ALTER TABLE `tb_hbheuprskb` CHANGE COLUMN `col_lxqjgyhhxu` `col_xxgthtdavh` datetime NOT NULL FIRST;
ALTER TABLE `tb_hbheuprskb` CHANGE `col_fdqpihurmr` `col_wlrahixbmg` blob(812190565);
ALTER TABLE `tb_hbheuprskb` CHANGE COLUMN `col_owakmhospa` `col_riqgldqhjp` integer NULL DEFAULT '1';
ALTER TABLE `tb_hbheuprskb` DROP COLUMN `col_mmxaoivplw`;
ALTER TABLE `tb_hbheuprskb` DROP COLUMN `col_wlrahixbmg`;
ALTER TABLE `tb_hbheuprskb` DROP `col_qdrqmqehyb`;
ALTER TABLE `tb_hbheuprskb` DROP `col_xxgthtdavh`;
ALTER TABLE `tb_hbheuprskb` DROP `col_riqgldqhjp`;
