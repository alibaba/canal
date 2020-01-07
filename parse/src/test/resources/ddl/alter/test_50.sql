CREATE TABLE `tb_pksakrzcnn` (
  `col_gtrcjlfnjb` tinyblob,
  UNIQUE `col_gtrcjlfnjb` (`col_gtrcjlfnjb`(30)),
  UNIQUE `uk_oobktihzsw` (`col_gtrcjlfnjb`(28))
) DEFAULT CHARSET=utf8;
CREATE TABLE `tb_tbfvbntpjg` (
  `col_qqccessygq` year NOT NULL,
  PRIMARY KEY (`col_qqccessygq`),
  UNIQUE `uk_mffeuwunua` (`col_qqccessygq`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
RENAME TABLE `tb_tbfvbntpjg` TO `tb_rhtejtaerv`;
RENAME TABLE `tb_pksakrzcnn` TO `tb_cfnewnjqhr`, `tb_rhtejtaerv` TO `tb_naclwnujgs`;
ALTER TABLE `tb_cfnewnjqhr` ADD (`col_efozqsfkcb` bit, `col_itpqtfoddn` tinytext CHARACTER SET utf8mb4);
ALTER TABLE `tb_cfnewnjqhr` ADD (`col_givbhjmmfw` smallint(231) zerofill, `col_bvndstnucz` char);
ALTER TABLE `tb_cfnewnjqhr` ADD COLUMN (`col_ongadrkgse` mediumblob, `col_cdwqlqfwox` int unsigned NULL DEFAULT '1');
ALTER TABLE `tb_cfnewnjqhr` DEFAULT CHARACTER SET utf8;
ALTER TABLE `tb_cfnewnjqhr` ADD UNIQUE (`col_gtrcjlfnjb`(11),`col_efozqsfkcb`);
ALTER TABLE `tb_cfnewnjqhr` CHANGE `col_itpqtfoddn` `col_zunavirely` bit NULL FIRST;
ALTER TABLE `tb_cfnewnjqhr` DROP COLUMN `col_bvndstnucz`, DROP COLUMN `col_zunavirely`;
ALTER TABLE `tb_cfnewnjqhr` DROP `col_ongadrkgse`, DROP `col_efozqsfkcb`;
ALTER TABLE `tb_cfnewnjqhr` DROP COLUMN `col_gtrcjlfnjb`;
ALTER TABLE `tb_cfnewnjqhr` DROP `col_givbhjmmfw`;
