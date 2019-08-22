CREATE TABLE `tb_gphkzjggwo` (
  `col_gmbhpsrxst` int(165) NULL DEFAULT '1',
  `col_cfefurpvif` int unsigned zerofill,
  UNIQUE `col_cfefurpvif` (`col_cfefurpvif`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE TABLE `tb_kfmwkcront` LIKE `tb_gphkzjggwo`;
RENAME TABLE `tb_gphkzjggwo` TO `tb_swbjpgvwuf`;
RENAME TABLE `tb_swbjpgvwuf` TO `tb_hdbseivabf`;
RENAME TABLE `tb_kfmwkcront` TO `tb_mnqvagcyml`, `tb_hdbseivabf` TO `tb_eswbmodety`;
DROP TABLE tb_eswbmodety, tb_mnqvagcyml;
CREATE TABLE `tb_gcekkribdm` (
  `col_vbxxcedehr` varbinary(72) DEFAULT '\0',
  `col_spnkzcnaaj` tinyint(221) unsigned zerofill NULL,
  `col_vdafmpnlcj` timestamp NULL,
  `col_pwwnuhvonf` bit NULL,
  UNIQUE `uk_oyduszzkbw` (`col_vdafmpnlcj`,`col_pwwnuhvonf`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
ALTER TABLE `tb_gcekkribdm` ADD COLUMN (`col_wpdnfgwkmm` float(253,19) NOT NULL, `col_hmbydpntpk` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') DEFAULT 'enum_or_set_0');
ALTER TABLE `tb_gcekkribdm` ADD (`col_mrruauiuzg` mediumblob, `col_cxafkgwrzu` varbinary(36));
ALTER TABLE `tb_gcekkribdm` ADD COLUMN (`col_ooxuaiyuqa` mediumint(107) unsigned zerofill NULL, `col_mxpgmnrigf` decimal);
ALTER TABLE `tb_gcekkribdm` ADD COLUMN (`col_sgjbftdxrq` mediumtext, `col_zboghrfujc` char(34) CHARACTER SET utf8mb4);
ALTER TABLE `tb_gcekkribdm` ADD COLUMN `col_doacvjtxfj` text FIRST;
ALTER TABLE `tb_gcekkribdm` ADD (`col_xpwlcesvfx` integer(73), `col_inpipqloqk` smallint zerofill NULL);
ALTER TABLE `tb_gcekkribdm` ADD `col_kahmeuqzzo` mediumtext CHARACTER SET utf8mb4 AFTER `col_hmbydpntpk`;
ALTER TABLE `tb_gcekkribdm` ADD COLUMN `col_xpuntwyhls` int(253) zerofill FIRST;
ALTER TABLE `tb_gcekkribdm` ADD (`col_rhqisewrfr` binary(209) NULL, `col_dznwcqadnc` mediumtext);
ALTER TABLE `tb_gcekkribdm` CHARACTER SET utf8mb4;
ALTER TABLE `tb_gcekkribdm` CHANGE COLUMN `col_inpipqloqk` `col_lctlnulyqs` varchar(141) CHARACTER SET utf8 DEFAULT '' AFTER `col_spnkzcnaaj`;
ALTER TABLE `tb_gcekkribdm` CHANGE `col_rhqisewrfr` `col_xpdyiqxtxg` decimal(62);
ALTER TABLE `tb_gcekkribdm` CHANGE `col_xpdyiqxtxg` `col_ysqvnorogf` int unsigned DEFAULT '1';
ALTER TABLE `tb_gcekkribdm` DROP KEY `uk_oyduszzkbw`;
