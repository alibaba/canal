CREATE TABLE `tb_jhzyfvcckl` (
  `col_qintmgglbx` longblob,
  `col_ovkqmnppgs` varchar(173) CHARACTER SET utf8 NOT NULL DEFAULT '',
  CONSTRAINT PRIMARY KEY (`col_ovkqmnppgs`(20))
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `tb_lbmxsuqwfl` LIKE `tb_jhzyfvcckl`;
RENAME TABLE `tb_lbmxsuqwfl` TO `tb_xexteouuua`, `tb_jhzyfvcckl` TO `tb_ifclwhlshu`;
RENAME TABLE `tb_ifclwhlshu` TO `tb_kuxfqvurrv`;
RENAME TABLE `tb_xexteouuua` TO `tb_sbjgtoodpq`;
DROP TABLE tb_kuxfqvurrv;
ALTER TABLE `tb_sbjgtoodpq` ADD (`col_qarkfvqqbu` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') DEFAULT 'enum_or_set_0', `col_qftesnwcek` blob);
ALTER TABLE `tb_sbjgtoodpq` ADD COLUMN (`col_ihnqibzahr` timestamp, `col_vrqkkfmczz` datetime(3));
ALTER TABLE `tb_sbjgtoodpq` ADD COLUMN (`col_rsjadrmlhn` varchar(202) CHARACTER SET utf8mb4 NOT NULL, `col_npydnongql` decimal(45) NOT NULL);
ALTER TABLE `tb_sbjgtoodpq` ADD COLUMN (`col_pfssvgbbli` bigint unsigned zerofill NULL, `col_gygioimzsp` char(93) NOT NULL);
ALTER TABLE `tb_sbjgtoodpq` ADD `col_qqyenpblbe` binary;
ALTER TABLE `tb_sbjgtoodpq` ADD COLUMN (`col_mkayxjoxkl` binary(229), `col_zlrfenamiz` float(28) NULL);
ALTER TABLE `tb_sbjgtoodpq` ADD COLUMN (`col_nghfeukgiz` tinyint(108) unsigned NOT NULL DEFAULT '1', `col_aocdtcdbuv` longtext);
ALTER TABLE `tb_sbjgtoodpq` ADD UNIQUE `uk_eikmgzebja` (`col_ovkqmnppgs`(31),`col_rsjadrmlhn`(24));
ALTER TABLE `tb_sbjgtoodpq` ADD UNIQUE KEY `col_mkayxjoxkl`(`col_mkayxjoxkl`(27),`col_nghfeukgiz`);
ALTER TABLE `tb_sbjgtoodpq` ALTER COLUMN `col_qqyenpblbe` DROP DEFAULT;
ALTER TABLE `tb_sbjgtoodpq` ALTER COLUMN `col_zlrfenamiz` DROP DEFAULT;
ALTER TABLE `tb_sbjgtoodpq` CHANGE `col_rsjadrmlhn` `col_ebmqvifose` bit DEFAULT b'0' AFTER `col_ovkqmnppgs`;
ALTER TABLE `tb_sbjgtoodpq` CHANGE COLUMN `col_qftesnwcek` `col_btsvyjliwe` longtext CHARACTER SET utf8 AFTER `col_gygioimzsp`;
ALTER TABLE `tb_sbjgtoodpq` DROP COLUMN `col_vrqkkfmczz`;
ALTER TABLE `tb_sbjgtoodpq` DROP `col_nghfeukgiz`;
ALTER TABLE `tb_sbjgtoodpq` DROP COLUMN `col_qarkfvqqbu`;
ALTER TABLE `tb_sbjgtoodpq` DROP COLUMN `col_gygioimzsp`;
ALTER TABLE `tb_sbjgtoodpq` DROP `col_qintmgglbx`, DROP `col_ebmqvifose`;
ALTER TABLE `tb_sbjgtoodpq` DROP `col_aocdtcdbuv`;
ALTER TABLE `tb_sbjgtoodpq` DROP `col_ovkqmnppgs`;
ALTER TABLE `tb_sbjgtoodpq` DROP COLUMN `col_ihnqibzahr`, DROP COLUMN `col_btsvyjliwe`;
ALTER TABLE `tb_sbjgtoodpq` DROP KEY `col_mkayxjoxkl`;
