CREATE TABLE `tb_mfkmsjoicz` (
  `col_wgoqxgzaqj` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8 DEFAULT 'enum_or_set_0',
  `col_fdlnxqretm` tinyblob,
  UNIQUE INDEX `uk_qvzxxvonmt` (`col_wgoqxgzaqj`),
  UNIQUE INDEX `col_wgoqxgzaqj` (`col_wgoqxgzaqj`)
) DEFAULT CHARSET=utf8;
RENAME TABLE `tb_mfkmsjoicz` TO `tb_cqcpkwgqnw`;
RENAME TABLE `tb_cqcpkwgqnw` TO `tb_wqthgwmnob`;
ALTER TABLE `tb_wqthgwmnob` ADD `col_ekbblvxoft` longblob;
ALTER TABLE `tb_wqthgwmnob` ADD COLUMN `col_lgwmegdicv` date NULL DEFAULT '2019-07-04';
ALTER TABLE `tb_wqthgwmnob` CHANGE `col_fdlnxqretm` `col_zfwcxazxyk` bit NOT NULL DEFAULT b'0';
ALTER TABLE `tb_wqthgwmnob` CHANGE `col_lgwmegdicv` `col_enozfpzijr` date;
ALTER TABLE `tb_wqthgwmnob` DROP `col_zfwcxazxyk`;
ALTER TABLE `tb_wqthgwmnob` DROP `col_ekbblvxoft`;
ALTER TABLE `tb_wqthgwmnob` DROP `col_wgoqxgzaqj`;
