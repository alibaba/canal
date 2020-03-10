CREATE TABLE `tb_zauuwyxibd` (
  `col_rbwuihwtww` float NULL DEFAULT '1',
  `col_zopsszwsjh` numeric DEFAULT '1',
  UNIQUE `col_zopsszwsjh` (`col_zopsszwsjh`)
) DEFAULT CHARSET=utf8;
RENAME TABLE `tb_zauuwyxibd` TO `tb_cdvsermcmz`;
RENAME TABLE `tb_cdvsermcmz` TO `tb_fijiqnymmp`;
ALTER TABLE `tb_fijiqnymmp` ADD COLUMN (`col_xxjnkwqqms` varbinary(216) DEFAULT '\0', `col_rhkizkifwy` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8 NULL);
ALTER TABLE `tb_fijiqnymmp` CHANGE COLUMN `col_xxjnkwqqms` `col_tcroqoomon` tinyblob AFTER `col_rbwuihwtww`;
ALTER TABLE `tb_fijiqnymmp` CHANGE `col_zopsszwsjh` `col_wontrujmna` year(4) DEFAULT '2019' AFTER `col_rbwuihwtww`;
ALTER TABLE `tb_fijiqnymmp` DROP COLUMN `col_rhkizkifwy`;
ALTER TABLE `tb_fijiqnymmp` DROP `col_wontrujmna`, DROP `col_rbwuihwtww`;
