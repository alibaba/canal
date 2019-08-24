CREATE TABLE `tb_rbpnonwcoi` (
  `col_efmpbyjrdf` year NULL,
  UNIQUE `col_efmpbyjrdf` (`col_efmpbyjrdf`),
  UNIQUE `col_efmpbyjrdf_2` (`col_efmpbyjrdf`)
) DEFAULT CHARSET=utf8;
CREATE TABLE `tb_meinmfimcf` (
  `col_xfazfienqr` datetime DEFAULT '2019-07-04 00:00:00',
  `col_vwufqrmckr` date NOT NULL,
  PRIMARY KEY (`col_vwufqrmckr`)
) DEFAULT CHARSET=utf8;
CREATE TABLE `tb_nnvqjcjwfg` LIKE `tb_rbpnonwcoi`;
RENAME TABLE `tb_nnvqjcjwfg` TO `tb_forjngzrrh`, `tb_meinmfimcf` TO `tb_mbjktaoecs`;
RENAME TABLE `tb_mbjktaoecs` TO `tb_trprduwltr`, `tb_forjngzrrh` TO `tb_sdpdmvgtcg`;
DROP TABLE tb_rbpnonwcoi, tb_sdpdmvgtcg;
DROP TABLE tb_trprduwltr;
CREATE TABLE `tb_ntoztqvpgu` (
  `col_pszlqmpajg` mediumint unsigned NOT NULL,
  `col_gsplwknfrh` timestamp,
  UNIQUE `uk_tjprvpapej` (`col_pszlqmpajg`,`col_gsplwknfrh`),
  UNIQUE INDEX `uk_bkvacomalw` (`col_pszlqmpajg`)
) DEFAULT CHARSET=latin1;
ALTER TABLE `tb_ntoztqvpgu` ADD COLUMN (`col_xzoqslbemg` int DEFAULT '1', `col_ytfasfxidt` mediumtext CHARACTER SET utf8mb4);
ALTER TABLE `tb_ntoztqvpgu` ADD COLUMN (`col_guxaslfjra` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') DEFAULT 'enum_or_set_0', `col_hzazyxoqhs` int(240) unsigned DEFAULT '1');
ALTER TABLE `tb_ntoztqvpgu` ADD `col_yrswgxnlhe` mediumtext CHARACTER SET utf8;
ALTER TABLE `tb_ntoztqvpgu` ADD COLUMN (`col_kcynrqhwrh` tinyblob, `col_guruxuyivx` char CHARACTER SET utf8mb4);
ALTER TABLE `tb_ntoztqvpgu` ADD COLUMN (`col_bpigfcahks` longblob, `col_xzbtbplmxx` float(173,30) NULL);
ALTER TABLE `tb_ntoztqvpgu` ADD `col_aklhemsftu` datetime DEFAULT '2019-07-04 00:00:00' AFTER `col_guruxuyivx`;
ALTER TABLE `tb_ntoztqvpgu` ADD (`col_abtzsnchmp` time(3) NULL, `col_xrqqlwdmuo` datetime NOT NULL);
ALTER TABLE `tb_ntoztqvpgu` ADD COLUMN (`col_yesjtrmdcm` smallint unsigned DEFAULT '1', `col_szbvvxlxpk` mediumtext);
ALTER TABLE `tb_ntoztqvpgu` ADD (`col_qbvmzghubk` longblob, `col_zvbkjkirrz` mediumint(68) unsigned DEFAULT '1');
ALTER TABLE `tb_ntoztqvpgu` CHARACTER SET = utf8;
ALTER TABLE `tb_ntoztqvpgu` CHANGE `col_xzoqslbemg` `col_bogirlzjnd` float AFTER `col_yrswgxnlhe`;
ALTER TABLE `tb_ntoztqvpgu` DROP INDEX `uk_tjprvpapej`;
ALTER TABLE `tb_ntoztqvpgu` DROP INDEX `uk_bkvacomalw`;
