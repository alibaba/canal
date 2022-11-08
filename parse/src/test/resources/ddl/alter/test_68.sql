CREATE TABLE `tb_febzciaoij` (
  `col_cchheryffd` mediumint unsigned NULL,
  `col_emlschxjrp` mediumblob,
  `col_qjiterpuli` binary(101),
  `col_icnjklhceu` numeric(12) NOT NULL,
  CONSTRAINT PRIMARY KEY (`col_icnjklhceu`)
) DEFAULT CHARSET=latin1;
CREATE TABLE `tb_vmghxdnngw` (
  `col_qbfketzeju` smallint(77),
  `col_xwdguyhvwy` bit NULL DEFAULT b'0'
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE TABLE `tb_xyqwqnvfml` (
  `col_bfzzufqfye` tinyint(197) zerofill,
  UNIQUE `uk_hpymxvxhzj` (`col_bfzzufqfye`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
RENAME TABLE `tb_xyqwqnvfml` TO `tb_gfcldozzsj`;
DROP TABLE tb_gfcldozzsj, tb_febzciaoij;
ALTER TABLE `tb_vmghxdnngw` ADD `col_mhpxueotyg` year(4) NULL FIRST;
ALTER TABLE `tb_vmghxdnngw` ADD (`col_mrbemxbmct` numeric(30), `col_yugqsxovwb` timestamp(2) NULL);
ALTER TABLE `tb_vmghxdnngw` CHARACTER SET = utf8mb4;
ALTER TABLE `tb_vmghxdnngw` ALTER COLUMN `col_xwdguyhvwy` SET DEFAULT b'0';
ALTER TABLE `tb_vmghxdnngw` ALTER `col_mhpxueotyg` DROP DEFAULT;
ALTER TABLE `tb_vmghxdnngw` CHANGE COLUMN `col_qbfketzeju` `col_jwubgoycad` decimal(7,4) NOT NULL AFTER `col_xwdguyhvwy`;
ALTER TABLE `tb_vmghxdnngw` CHANGE `col_jwubgoycad` `col_cuofmwfcxy` tinytext;
ALTER TABLE `tb_vmghxdnngw` CHANGE `col_xwdguyhvwy` `col_itolrjernl` bit(57) NULL AFTER `col_yugqsxovwb`;
ALTER TABLE `tb_vmghxdnngw` DROP `col_mrbemxbmct`, DROP `col_itolrjernl`;
ALTER TABLE `tb_vmghxdnngw` DROP COLUMN `col_yugqsxovwb`, DROP COLUMN `col_mhpxueotyg`;
