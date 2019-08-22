CREATE TABLE `tb_tmeujebigu` (
  `col_vexgpiotuy` binary,
  `col_uouidnfxfe` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
RENAME TABLE `tb_tmeujebigu` TO `tb_vqlldbakax`;
RENAME TABLE `tb_vqlldbakax` TO `tb_xduxzgfsux`;
RENAME TABLE `tb_xduxzgfsux` TO `tb_yfhnotrnrt`;
ALTER TABLE `tb_yfhnotrnrt` ADD COLUMN (`col_cueciidhfy` timestamp(1) NULL DEFAULT CURRENT_TIMESTAMP(1), `col_xebqcglpvr` char);
ALTER TABLE `tb_yfhnotrnrt` ADD (`col_cvhurmqyru` tinyint(52) zerofill NOT NULL, `col_gdsggqabpa` mediumblob);
ALTER TABLE `tb_yfhnotrnrt` ADD (`col_mstusvmhpu` tinytext CHARACTER SET utf8mb4, `col_dyphdacvvd` tinytext CHARACTER SET utf8mb4);
ALTER TABLE `tb_yfhnotrnrt` ADD COLUMN (`col_ulwbtrmfes` smallint unsigned DEFAULT '1', `col_xgueitftmx` float(247,18));
ALTER TABLE `tb_yfhnotrnrt` ADD (`col_spogbqhjrx` date NOT NULL, `col_ormwjqfbxi` int(193) unsigned zerofill NULL);
ALTER TABLE `tb_yfhnotrnrt` ADD COLUMN `col_iruzsjhmbm` varbinary(83) DEFAULT '\0';
ALTER TABLE `tb_yfhnotrnrt` CHARACTER SET utf8;
ALTER TABLE `tb_yfhnotrnrt` ALTER COLUMN `col_iruzsjhmbm` DROP DEFAULT;
ALTER TABLE `tb_yfhnotrnrt` ALTER COLUMN `col_ormwjqfbxi` SET DEFAULT NULL;
ALTER TABLE `tb_yfhnotrnrt` ALTER COLUMN `col_xgueitftmx` DROP DEFAULT;
ALTER TABLE `tb_yfhnotrnrt` CHANGE `col_ulwbtrmfes` `col_kednlljjiu` date NOT NULL DEFAULT '2019-07-04' AFTER `col_gdsggqabpa`;
ALTER TABLE `tb_yfhnotrnrt` CHANGE `col_xebqcglpvr` `col_qslraszfyz` longtext CHARACTER SET utf8mb4;
ALTER TABLE `tb_yfhnotrnrt` CHANGE `col_kednlljjiu` `col_irqdemfadw` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8mb4 DEFAULT 'enum_or_set_0';
ALTER TABLE `tb_yfhnotrnrt` DROP `col_gdsggqabpa`;
ALTER TABLE `tb_yfhnotrnrt` DROP COLUMN `col_uouidnfxfe`;
ALTER TABLE `tb_yfhnotrnrt` DROP COLUMN `col_irqdemfadw`;
ALTER TABLE `tb_yfhnotrnrt` DROP `col_cueciidhfy`, DROP `col_mstusvmhpu`;
ALTER TABLE `tb_yfhnotrnrt` DROP COLUMN `col_spogbqhjrx`;
ALTER TABLE `tb_yfhnotrnrt` DROP `col_iruzsjhmbm`, DROP `col_cvhurmqyru`;
