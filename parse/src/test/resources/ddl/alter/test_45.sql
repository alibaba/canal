CREATE TABLE `tb_pvcychmnrw` (
  `col_xgykdmygaw` tinyblob
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE TABLE `tb_hidugktlpo` LIKE `tb_pvcychmnrw`;
RENAME TABLE `tb_pvcychmnrw` TO `tb_bevepbdlfp`;
RENAME TABLE `tb_hidugktlpo` TO `tb_nnupayyotj`;
RENAME TABLE `tb_bevepbdlfp` TO `tb_voanuixent`, `tb_nnupayyotj` TO `tb_znqepghruj`;
ALTER TABLE `tb_znqepghruj` ADD COLUMN (`col_lapmiukuzm` bigint(4) unsigned zerofill, `col_jncrcdzmcj` varchar(224) NULL);
ALTER TABLE `tb_znqepghruj` ADD `col_yozqdpmolb` mediumblob AFTER `col_lapmiukuzm`;
ALTER TABLE `tb_znqepghruj` ADD `col_jfuyjljjqa` longtext CHARACTER SET utf8mb4 FIRST;
ALTER TABLE `tb_znqepghruj` ADD `col_vpxahsfmgv` longtext CHARACTER SET utf8 COLLATE utf8_unicode_ci;
ALTER TABLE `tb_znqepghruj` ADD COLUMN `col_utlqihmybp` varbinary(41) NULL AFTER `col_lapmiukuzm`;
ALTER TABLE `tb_znqepghruj` ADD UNIQUE (`col_xgykdmygaw`(21),`col_utlqihmybp`(30));
ALTER TABLE `tb_znqepghruj` ADD UNIQUE KEY `uk_bkrpsgutim` (`col_jfuyjljjqa`(18),`col_utlqihmybp`(3));
ALTER TABLE `tb_znqepghruj` ALTER `col_utlqihmybp` SET DEFAULT NULL;
ALTER TABLE `tb_znqepghruj` CHANGE COLUMN `col_jncrcdzmcj` `col_ekvpssddsv` year FIRST;
ALTER TABLE `tb_znqepghruj` CHANGE `col_yozqdpmolb` `col_uzlftjvzzr` mediumint(64) zerofill FIRST;
ALTER TABLE `tb_znqepghruj` DROP `col_jfuyjljjqa`, DROP `col_ekvpssddsv`;
ALTER TABLE `tb_znqepghruj` DROP `col_utlqihmybp`;
ALTER TABLE `tb_znqepghruj` DROP COLUMN `col_vpxahsfmgv`;
ALTER TABLE `tb_znqepghruj` DROP `col_lapmiukuzm`, DROP `col_uzlftjvzzr`;
