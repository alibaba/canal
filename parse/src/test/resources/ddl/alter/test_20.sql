CREATE TABLE `tb_zjdvakwwwv` (
  `col_bytftanpdh` mediumint(56) unsigned DEFAULT '1',
  `col_zwyhucxxkr` year(4) NOT NULL,
  `col_desbhtchpe` text(3076914288) CHARACTER SET latin1,
  `col_figelwsuqt` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET latin1 DEFAULT 'enum_or_set_0',
  UNIQUE `col_desbhtchpe` (`col_desbhtchpe`(5)),
  UNIQUE `col_bytftanpdh` (`col_bytftanpdh`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
RENAME TABLE `tb_zjdvakwwwv` TO `tb_unztlaanoa`;
RENAME TABLE `tb_unztlaanoa` TO `tb_wxywfqcvkr`;
ALTER TABLE `tb_wxywfqcvkr` ADD COLUMN (`col_hikfliwljt` varchar(14), `col_vrnocgbjsc` mediumtext CHARACTER SET utf8);
ALTER TABLE `tb_wxywfqcvkr` ADD COLUMN (`col_gudluixdai` char, `col_adpuaqjrtd` numeric(22,2) NOT NULL);
ALTER TABLE `tb_wxywfqcvkr` ADD COLUMN `col_vjvhvsxmoi` bigint zerofill AFTER `col_desbhtchpe`;
ALTER TABLE `tb_wxywfqcvkr` ADD COLUMN (`col_whowxgsfzl` blob(1844532465), `col_mzdqafxiqx` bigint(161) unsigned DEFAULT '1');
ALTER TABLE `tb_wxywfqcvkr` ADD `col_spyfsutunk` bit DEFAULT b'0' AFTER `col_zwyhucxxkr`;
ALTER TABLE `tb_wxywfqcvkr` ADD `col_iqugcuomsl` numeric(9) NOT NULL;
ALTER TABLE `tb_wxywfqcvkr` ADD PRIMARY KEY (`col_zwyhucxxkr`,`col_adpuaqjrtd`);
ALTER TABLE `tb_wxywfqcvkr` CHANGE `col_zwyhucxxkr` `col_sixmujutpp` smallint zerofill NOT NULL AFTER `col_hikfliwljt`;
ALTER TABLE `tb_wxywfqcvkr` CHANGE COLUMN `col_hikfliwljt` `col_oftatmlkzc` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') NOT NULL FIRST;
ALTER TABLE `tb_wxywfqcvkr` CHANGE COLUMN `col_spyfsutunk` `col_wwaeohiyto` timestamp FIRST;
ALTER TABLE `tb_wxywfqcvkr` DROP `col_adpuaqjrtd`;
ALTER TABLE `tb_wxywfqcvkr` DROP `col_iqugcuomsl`, DROP `col_sixmujutpp`;
ALTER TABLE `tb_wxywfqcvkr` DROP `col_vrnocgbjsc`;
ALTER TABLE `tb_wxywfqcvkr` DROP `col_wwaeohiyto`;
ALTER TABLE `tb_wxywfqcvkr` DROP `col_bytftanpdh`;
ALTER TABLE `tb_wxywfqcvkr` DROP `col_desbhtchpe`;
ALTER TABLE `tb_wxywfqcvkr` DROP COLUMN `col_gudluixdai`;
