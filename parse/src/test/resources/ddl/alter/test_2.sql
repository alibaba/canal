CREATE TABLE `tb_kyvyqhwcpi` (
  `col_pesoyggsjl` mediumblob,
  `col_hctgmkmyxs` datetime DEFAULT '2019-07-04 00:00:00',
  `col_kkvhrfifbo` longtext CHARACTER SET utf8,
  `col_ewyzkapekq` longblob,
  UNIQUE KEY `uk_jvoonsdsfr` (`col_pesoyggsjl`(32)),
  UNIQUE INDEX `col_kkvhrfifbo` (`col_kkvhrfifbo`(31))
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
RENAME TABLE `tb_kyvyqhwcpi` TO `tb_zyynbdsyib`;
RENAME TABLE `tb_zyynbdsyib` TO `tb_gcqyhapjeh`;
ALTER TABLE `tb_gcqyhapjeh` ADD `col_mlfdocnxzd` text CHARACTER SET utf8 FIRST;
ALTER TABLE `tb_gcqyhapjeh` ADD COLUMN (`col_yxhhhxgwjz` datetime(5) NOT NULL, `col_mqfdtybcrt` longtext);
ALTER TABLE `tb_gcqyhapjeh` ADD (`col_dszxjztuni` varbinary(68) NOT NULL DEFAULT '\0', `col_sukknvclub` longblob);
ALTER TABLE `tb_gcqyhapjeh` ADD COLUMN (`col_zhvifnzhjr` tinyint(107) zerofill, `col_fscylalync` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8);
ALTER TABLE `tb_gcqyhapjeh` ADD `col_ddgcxloigt` bit(6) NULL;
ALTER TABLE `tb_gcqyhapjeh` ADD (`col_ihidhmecqm` tinytext, `col_icjiguudiw` bigint zerofill);
ALTER TABLE `tb_gcqyhapjeh` ADD COLUMN `col_hxvzygxblr` binary;
ALTER TABLE `tb_gcqyhapjeh` ADD (`col_xiszwdtbgk` tinyint zerofill NOT NULL, `col_cdyttfqwra` longblob);
ALTER TABLE `tb_gcqyhapjeh` ADD `col_jzhdjycrpd` longtext AFTER `col_ewyzkapekq`;
ALTER TABLE `tb_gcqyhapjeh` ADD UNIQUE (`col_cdyttfqwra`(29));
ALTER TABLE `tb_gcqyhapjeh` ALTER COLUMN `col_icjiguudiw` DROP DEFAULT;
ALTER TABLE `tb_gcqyhapjeh` DROP `col_cdyttfqwra`;
ALTER TABLE `tb_gcqyhapjeh` DROP COLUMN `col_pesoyggsjl`, DROP COLUMN `col_sukknvclub`;
ALTER TABLE `tb_gcqyhapjeh` DROP `col_dszxjztuni`, DROP `col_xiszwdtbgk`;
ALTER TABLE `tb_gcqyhapjeh` DROP `col_ewyzkapekq`;
ALTER TABLE `tb_gcqyhapjeh` DROP COLUMN `col_mlfdocnxzd`, DROP COLUMN `col_zhvifnzhjr`;
ALTER TABLE `tb_gcqyhapjeh` DROP `col_jzhdjycrpd`;
ALTER TABLE `tb_gcqyhapjeh` DROP COLUMN `col_ihidhmecqm`, DROP COLUMN `col_yxhhhxgwjz`;
ALTER TABLE `tb_gcqyhapjeh` DROP `col_fscylalync`, DROP `col_hxvzygxblr`;
