CREATE TABLE `tb_yzmdpoldbu` (
  `col_cmeqnxgnwo` int NULL DEFAULT '1',
  `col_qgergaavlw` longtext CHARACTER SET utf8,
  UNIQUE `uk_mctazwqtlw` (`col_qgergaavlw`(14))
) DEFAULT CHARSET=utf8;
CREATE TABLE `tb_iuchtibsiu` (
  `col_qyxegyfxhd` binary NOT NULL,
  `col_fhrwjmkunp` time NULL DEFAULT '00:00:00',
  `col_ebdpffnuvl` longblob,
  `col_ivfpfrbagk` decimal(48),
  CONSTRAINT PRIMARY KEY (`col_qyxegyfxhd`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
RENAME TABLE `tb_iuchtibsiu` TO `tb_oxrpkkhlpd`;
ALTER TABLE `tb_yzmdpoldbu` ADD COLUMN (`col_qdzrqnbwug` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') NULL DEFAULT 'enum_or_set_0', `col_hdsuyxblyk` double(86,4));
ALTER TABLE `tb_yzmdpoldbu` ADD (`col_pabufpjxkv` tinytext, `col_ietwsffxgn` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2'));
ALTER TABLE `tb_yzmdpoldbu` ADD (`col_hvdglgtlxp` longblob, `col_bwfgathikh` integer unsigned);
ALTER TABLE `tb_yzmdpoldbu` ADD COLUMN `col_sdagljipnl` tinyblob AFTER `col_cmeqnxgnwo`;
ALTER TABLE `tb_yzmdpoldbu` ADD (`col_yenjodhatg` year NULL DEFAULT '2019', `col_fefjkkdwwg` tinytext);
ALTER TABLE `tb_yzmdpoldbu` ADD (`col_bhkqbxsemo` tinytext CHARACTER SET utf8mb4, `col_cwmggcyoud` numeric NULL);
ALTER TABLE `tb_yzmdpoldbu` ADD COLUMN `col_hrwaxjpjgr` binary NOT NULL;
ALTER TABLE `tb_yzmdpoldbu` ADD UNIQUE `uk_bmkecbeunv` (`col_fefjkkdwwg`(12),`col_cwmggcyoud`);
ALTER TABLE `tb_yzmdpoldbu` ALTER COLUMN `col_bwfgathikh` DROP DEFAULT;
ALTER TABLE `tb_yzmdpoldbu` CHANGE COLUMN `col_bhkqbxsemo` `col_kloxucvkgw` bigint(200) unsigned DEFAULT '1' AFTER `col_cwmggcyoud`;
ALTER TABLE `tb_yzmdpoldbu` CHANGE COLUMN `col_yenjodhatg` `col_pbngzmfduy` varchar(225) NOT NULL;
ALTER TABLE `tb_yzmdpoldbu` CHANGE COLUMN `col_cwmggcyoud` `col_prltnqtpwl` datetime(3);
ALTER TABLE `tb_yzmdpoldbu` DROP COLUMN `col_ietwsffxgn`, DROP COLUMN `col_hrwaxjpjgr`;
ALTER TABLE `tb_yzmdpoldbu` DROP `col_hdsuyxblyk`;
ALTER TABLE `tb_yzmdpoldbu` DROP `col_prltnqtpwl`, DROP `col_sdagljipnl`;
ALTER TABLE `tb_yzmdpoldbu` DROP `col_bwfgathikh`, DROP `col_qgergaavlw`;
