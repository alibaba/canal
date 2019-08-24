CREATE TABLE `tb_ijmcdmzknr` (
  `col_pqnfhpfywv` datetime DEFAULT '2019-07-04 00:00:00',
  `col_mlpkfxnbfa` blob,
  UNIQUE `uk_ctetrugpen` (`col_mlpkfxnbfa`(8)),
  UNIQUE KEY `col_mlpkfxnbfa` (`col_mlpkfxnbfa`(23))
) DEFAULT CHARSET=utf8;
RENAME TABLE `tb_ijmcdmzknr` TO `tb_mzqlybwqbo`;
RENAME TABLE `tb_mzqlybwqbo` TO `tb_gnbqbndsaw`;
ALTER TABLE `tb_gnbqbndsaw` ADD (`col_gpjsqkkxse` integer unsigned NULL DEFAULT '1', `col_gmjtupvuvl` text(644744023));
ALTER TABLE `tb_gnbqbndsaw` ADD UNIQUE INDEX `uk_cbwijpiehr` (`col_pqnfhpfywv`,`col_gpjsqkkxse`);
ALTER TABLE `tb_gnbqbndsaw` ADD UNIQUE (`col_mlpkfxnbfa`(18));
ALTER TABLE `tb_gnbqbndsaw` DROP `col_gpjsqkkxse`;
ALTER TABLE `tb_gnbqbndsaw` DROP COLUMN `col_mlpkfxnbfa`, DROP COLUMN `col_gmjtupvuvl`;
ALTER TABLE `tb_gnbqbndsaw` DROP KEY `uk_cbwijpiehr`;
