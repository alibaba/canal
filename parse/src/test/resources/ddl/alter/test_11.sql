CREATE TABLE `tb_urvrplctvm` (
  `col_nhgbwyjskp` timestamp(3) NULL,
  `col_tafveczsvk` datetime(5),
  `col_kzrjsjtgnj` bigint(192) zerofill,
  UNIQUE INDEX `col_nhgbwyjskp` (`col_nhgbwyjskp`),
  UNIQUE `uk_zizxxnbpva` (`col_nhgbwyjskp`)
) DEFAULT CHARSET=utf8;
CREATE TABLE `tb_ugiidczptx` (
  `col_fklmphgxdx` mediumtext CHARACTER SET utf8,
  UNIQUE KEY `col_fklmphgxdx` (`col_fklmphgxdx`(1))
) DEFAULT CHARSET=utf8;
CREATE TABLE `tb_ehakozpvcb` LIKE `tb_ugiidczptx`;
RENAME TABLE `tb_ehakozpvcb` TO `tb_qgeelwokgc`;
DROP TABLE tb_ugiidczptx, tb_qgeelwokgc;
DROP TABLE tb_urvrplctvm;
CREATE TABLE `tb_ytaiteijaz` (
  `col_tudnbijtqu` time NULL,
  `col_lxtrjmhpmr` year,
  `col_wvvanapjlz` timestamp NOT NULL,
  PRIMARY KEY (`col_wvvanapjlz`),
  UNIQUE `uk_pffwfanush` (`col_wvvanapjlz`)
) DEFAULT CHARSET=utf8;
ALTER TABLE `tb_ytaiteijaz` ADD (`col_rjguymssfi` tinyint(83) unsigned DEFAULT '1', `col_vapswtjoqo` smallint unsigned zerofill NOT NULL);
ALTER TABLE `tb_ytaiteijaz` ADD COLUMN (`col_xnljwkzjad` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') DEFAULT 'enum_or_set_0', `col_kajgusvxkq` smallint(75));
ALTER TABLE `tb_ytaiteijaz` ADD COLUMN `col_adlsfkuaee` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') AFTER `col_vapswtjoqo`;
ALTER TABLE `tb_ytaiteijaz` ADD `col_ohppccpxlg` datetime NOT NULL DEFAULT '2019-07-04 00:00:00';
ALTER TABLE `tb_ytaiteijaz` ADD `col_wougwncovl` decimal(28,22) NULL;
ALTER TABLE `tb_ytaiteijaz` ADD COLUMN (`col_qdhtjqsfti` mediumblob, `col_ryjkfjtwvk` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2'));
ALTER TABLE `tb_ytaiteijaz` ADD COLUMN `col_gcahrhoadr` tinyblob;
ALTER TABLE `tb_ytaiteijaz` ADD `col_ihtpuurapz` bit(28) AFTER `col_vapswtjoqo`;
ALTER TABLE `tb_ytaiteijaz` ADD COLUMN `col_agolscsuau` char(106) NULL;
ALTER TABLE `tb_ytaiteijaz` ADD UNIQUE KEY `col_kajgusvxkq`(`col_kajgusvxkq`,`col_wougwncovl`);
ALTER TABLE `tb_ytaiteijaz` ADD UNIQUE (`col_vapswtjoqo`);
ALTER TABLE `tb_ytaiteijaz` ALTER `col_wougwncovl` DROP DEFAULT;
ALTER TABLE `tb_ytaiteijaz` CHANGE COLUMN `col_gcahrhoadr` `col_huflgowphp` mediumint unsigned zerofill AFTER `col_tudnbijtqu`;
ALTER TABLE `tb_ytaiteijaz` CHANGE COLUMN `col_huflgowphp` `col_wghwzcsnsa` text CHARACTER SET utf8mb4 FIRST;
ALTER TABLE `tb_ytaiteijaz` CHANGE `col_tudnbijtqu` `col_pwojmgycov` bigint unsigned zerofill NULL AFTER `col_ohppccpxlg`;
ALTER TABLE `tb_ytaiteijaz` DROP `col_qdhtjqsfti`;
ALTER TABLE `tb_ytaiteijaz` DROP COLUMN `col_ohppccpxlg`;
ALTER TABLE `tb_ytaiteijaz` DROP COLUMN `col_wghwzcsnsa`;
ALTER TABLE `tb_ytaiteijaz` DROP COLUMN `col_adlsfkuaee`, DROP COLUMN `col_agolscsuau`;
ALTER TABLE `tb_ytaiteijaz` DROP COLUMN `col_ihtpuurapz`, DROP COLUMN `col_vapswtjoqo`;
ALTER TABLE `tb_ytaiteijaz` DROP COLUMN `col_ryjkfjtwvk`;
ALTER TABLE `tb_ytaiteijaz` DROP COLUMN `col_wougwncovl`, DROP COLUMN `col_rjguymssfi`;
ALTER TABLE `tb_ytaiteijaz` DROP PRIMARY KEY;
ALTER TABLE `tb_ytaiteijaz` DROP KEY `col_kajgusvxkq`;
