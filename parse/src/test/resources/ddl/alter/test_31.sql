CREATE TABLE `tb_oyptksgbxm` (
  `col_gdmzqziedn` mediumint(192) NULL DEFAULT '1',
  `col_iesufjnkzg` double(60,2) NOT NULL,
  `col_fpegtdbbro` varchar(10) CHARACTER SET latin1 NOT NULL DEFAULT '',
  CONSTRAINT symb_hwjsmtmrol PRIMARY KEY (`col_iesufjnkzg`),
  UNIQUE `uk_bbhokusgkq` (`col_gdmzqziedn`,`col_fpegtdbbro`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
RENAME TABLE `tb_oyptksgbxm` TO `tb_djhsoeomxx`;
ALTER TABLE `tb_djhsoeomxx` ADD COLUMN (`col_yuemiunaby` mediumtext, `col_ogwamnrrff` mediumint(37) DEFAULT '1');
ALTER TABLE `tb_djhsoeomxx` ADD `col_pvgwkvvphj` text(2345284065) CHARACTER SET utf8;
ALTER TABLE `tb_djhsoeomxx` CHARACTER SET = utf8mb4;
ALTER TABLE `tb_djhsoeomxx` ADD UNIQUE (`col_gdmzqziedn`);
ALTER TABLE `tb_djhsoeomxx` ADD UNIQUE INDEX `uk_pmearqrlzv` (`col_gdmzqziedn`,`col_yuemiunaby`(31));
ALTER TABLE `tb_djhsoeomxx` CHANGE COLUMN `col_gdmzqziedn` `col_mgdrjdeyro` char NOT NULL;
ALTER TABLE `tb_djhsoeomxx` DROP COLUMN `col_pvgwkvvphj`;
ALTER TABLE `tb_djhsoeomxx` DROP `col_ogwamnrrff`, DROP `col_iesufjnkzg`;
ALTER TABLE `tb_djhsoeomxx` DROP COLUMN `col_yuemiunaby`;
ALTER TABLE `tb_djhsoeomxx` DROP COLUMN `col_mgdrjdeyro`;
