CREATE TABLE `tb_vyhepkdocw` (
  `col_yqmzdxesrj` timestamp(0) NULL,
  `col_kraxstptzi` varbinary(174),
  `col_lvqcegncth` mediumblob,
  `col_slupqsxneh` numeric(14),
  UNIQUE KEY `uk_wqvouqksra` (`col_lvqcegncth`(8)),
  UNIQUE KEY `col_slupqsxneh` (`col_slupqsxneh`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE TABLE `tb_pektpgbgcx` LIKE `tb_vyhepkdocw`;
CREATE TABLE `tb_zeqwdifiwa` (
  `col_wovlphbjrk` text(2969615453) CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  UNIQUE INDEX `uk_eohdfnqdmx` (`col_wovlphbjrk`(28))
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
RENAME TABLE `tb_zeqwdifiwa` TO `tb_mwiswycksv`;
RENAME TABLE `tb_mwiswycksv` TO `tb_kzlrbhxfpu`;
DROP TABLE tb_pektpgbgcx, tb_kzlrbhxfpu;
ALTER TABLE `tb_vyhepkdocw` ADD `col_chvhleyulg` tinytext;
ALTER TABLE `tb_vyhepkdocw` ADD COLUMN `col_onhyboljxa` date NULL AFTER `col_lvqcegncth`;
ALTER TABLE `tb_vyhepkdocw` ADD (`col_getqnnhjry` date NULL, `col_dlomfzxpjh` mediumblob);
ALTER TABLE `tb_vyhepkdocw` ADD COLUMN `col_ahjuyzjbhk` bit AFTER `col_getqnnhjry`;
ALTER TABLE `tb_vyhepkdocw` ADD COLUMN (`col_hqbuexxcxl` bit, `col_mhqjuefjec` binary(213));
ALTER TABLE `tb_vyhepkdocw` ADD COLUMN (`col_daemlskcfa` double, `col_ewudcirsqc` tinyblob);
ALTER TABLE `tb_vyhepkdocw` ADD UNIQUE `col_daemlskcfa`(`col_onhyboljxa`,`col_slupqsxneh`);
ALTER TABLE `tb_vyhepkdocw` DROP `col_lvqcegncth`;
ALTER TABLE `tb_vyhepkdocw` DROP COLUMN `col_kraxstptzi`, DROP COLUMN `col_ewudcirsqc`;
ALTER TABLE `tb_vyhepkdocw` DROP COLUMN `col_hqbuexxcxl`;
ALTER TABLE `tb_vyhepkdocw` DROP `col_mhqjuefjec`;
ALTER TABLE `tb_vyhepkdocw` DROP `col_onhyboljxa`;
ALTER TABLE `tb_vyhepkdocw` DROP COLUMN `col_dlomfzxpjh`;
ALTER TABLE `tb_vyhepkdocw` DROP KEY `col_daemlskcfa`;
