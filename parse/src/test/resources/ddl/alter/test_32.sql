CREATE TABLE `tb_nnxufbaqsg` (
  `col_kytprnpyhw` decimal(15,15),
  `col_vhszikunhu` integer(95) unsigned zerofill,
  `col_llqqqlblzs` timestamp NOT NULL,
  `col_zlezapwnhv` double,
  UNIQUE `col_zlezapwnhv` (`col_zlezapwnhv`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
RENAME TABLE `tb_nnxufbaqsg` TO `tb_diurzhsdsd`;
RENAME TABLE `tb_diurzhsdsd` TO `tb_uhdpyibqnh`;
RENAME TABLE `tb_uhdpyibqnh` TO `tb_ybgjvtnrkt`;
ALTER TABLE `tb_ybgjvtnrkt` ADD COLUMN (`col_uuoufewntu` tinytext, `col_obvzxvrpac` year);
ALTER TABLE `tb_ybgjvtnrkt` ADD COLUMN (`col_qvxcdustin` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL, `col_qlfphmrfqo` longblob);
ALTER TABLE `tb_ybgjvtnrkt` ADD PRIMARY KEY (`col_llqqqlblzs`);
ALTER TABLE `tb_ybgjvtnrkt` ADD UNIQUE (`col_kytprnpyhw`);
ALTER TABLE `tb_ybgjvtnrkt` ADD UNIQUE INDEX `uk_flumuisvnj` (`col_qlfphmrfqo`(28));
ALTER TABLE `tb_ybgjvtnrkt` CHANGE `col_kytprnpyhw` `col_lcuznlvglo` time(4) AFTER `col_vhszikunhu`;
ALTER TABLE `tb_ybgjvtnrkt` DROP `col_obvzxvrpac`, DROP `col_qlfphmrfqo`;
ALTER TABLE `tb_ybgjvtnrkt` DROP `col_zlezapwnhv`;
ALTER TABLE `tb_ybgjvtnrkt` DROP COLUMN `col_qvxcdustin`, DROP COLUMN `col_uuoufewntu`;
ALTER TABLE `tb_ybgjvtnrkt` DROP COLUMN `col_lcuznlvglo`, DROP COLUMN `col_llqqqlblzs`;
