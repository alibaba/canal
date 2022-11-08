CREATE TABLE `tb_dkdeujiqjf` (
  `col_gwfnuyuami` mediumint zerofill,
  `col_jzcpwtvvhi` numeric NOT NULL DEFAULT '1',
  PRIMARY KEY (`col_jzcpwtvvhi`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
RENAME TABLE `tb_dkdeujiqjf` TO `tb_ehfacgfjnk`;
RENAME TABLE `tb_ehfacgfjnk` TO `tb_zwzsaathnd`;
ALTER TABLE `tb_zwzsaathnd` ADD COLUMN `col_axigbojqvl` float(34);
ALTER TABLE `tb_zwzsaathnd` ADD COLUMN (`col_vcejogynpm` time NULL, `col_rxmvdbomhz` time);
ALTER TABLE `tb_zwzsaathnd` ADD COLUMN (`col_nrzmqslqws` mediumtext, `col_dseumehqur` tinyblob);
ALTER TABLE `tb_zwzsaathnd` ADD `col_ojaoqduwkj` date DEFAULT '2019-07-04' FIRST;
ALTER TABLE `tb_zwzsaathnd` ADD `col_rqumywduzu` time FIRST;
ALTER TABLE `tb_zwzsaathnd` ADD `col_xxmdywhgie` mediumint NULL;
ALTER TABLE `tb_zwzsaathnd` CHARACTER SET = utf8;
ALTER TABLE `tb_zwzsaathnd` ADD UNIQUE KEY `col_rqumywduzu`(`col_rqumywduzu`,`col_ojaoqduwkj`);
ALTER TABLE `tb_zwzsaathnd` ADD UNIQUE INDEX `col_dseumehqur`(`col_dseumehqur`(21),`col_xxmdywhgie`);
ALTER TABLE `tb_zwzsaathnd` ALTER COLUMN `col_rxmvdbomhz` SET DEFAULT NULL;
ALTER TABLE `tb_zwzsaathnd` DROP COLUMN `col_ojaoqduwkj`;
ALTER TABLE `tb_zwzsaathnd` DROP `col_rxmvdbomhz`;
ALTER TABLE `tb_zwzsaathnd` DROP COLUMN `col_nrzmqslqws`;
ALTER TABLE `tb_zwzsaathnd` DROP COLUMN `col_axigbojqvl`, DROP COLUMN `col_gwfnuyuami`;
ALTER TABLE `tb_zwzsaathnd` DROP PRIMARY KEY;
ALTER TABLE `tb_zwzsaathnd` DROP INDEX `col_rqumywduzu`;
ALTER TABLE `tb_zwzsaathnd` DROP KEY `col_dseumehqur`;
