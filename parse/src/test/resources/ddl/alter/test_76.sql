CREATE TABLE `tb_wbajtotdth` (
  `col_lmpzmediez` int unsigned zerofill,
  `col_nhckjelmwe` year,
  `col_yvelqmjboo` varbinary(185) DEFAULT '\0',
  `col_uttbttlgkx` smallint(181) unsigned DEFAULT '1',
  UNIQUE KEY `col_uttbttlgkx` (`col_uttbttlgkx`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `tb_vrtkbtmnws` (
  `col_iabzixqyhm` tinyblob,
  `col_otoxjlwatj` date DEFAULT '2019-07-04',
  UNIQUE KEY `col_otoxjlwatj` (`col_otoxjlwatj`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
RENAME TABLE `tb_wbajtotdth` TO `tb_bxtlrumrtn`, `tb_vrtkbtmnws` TO `tb_irclltefag`;
ALTER TABLE `tb_bxtlrumrtn` CHARACTER SET = utf8mb4;
ALTER TABLE `tb_bxtlrumrtn` ALTER `col_nhckjelmwe` DROP DEFAULT;
ALTER TABLE `tb_bxtlrumrtn` ALTER `col_uttbttlgkx` SET DEFAULT NULL;
ALTER TABLE `tb_bxtlrumrtn` CHANGE COLUMN `col_nhckjelmwe` `col_dgjkrbzltd` numeric(38,25) NOT NULL;
ALTER TABLE `tb_bxtlrumrtn` DROP `col_lmpzmediez`;
ALTER TABLE `tb_bxtlrumrtn` DROP `col_yvelqmjboo`, DROP `col_dgjkrbzltd`;
ALTER TABLE `tb_bxtlrumrtn` DROP INDEX `col_uttbttlgkx`;
