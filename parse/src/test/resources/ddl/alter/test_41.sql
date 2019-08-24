CREATE TABLE `tb_lqvkbeqhuw` (
  `col_gseshvltxd` tinytext CHARACTER SET latin1,
  `col_otuwjnlrtt` decimal,
  UNIQUE INDEX `col_otuwjnlrtt` (`col_otuwjnlrtt`),
  UNIQUE `col_gseshvltxd` (`col_gseshvltxd`(26))
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
RENAME TABLE `tb_lqvkbeqhuw` TO `tb_mqghelxirz`;
ALTER TABLE `tb_mqghelxirz` ADD COLUMN (`col_gwwjlquinc` mediumint NOT NULL, `col_ibdwyzxgxi` varchar(134));
ALTER TABLE `tb_mqghelxirz` ADD COLUMN (`col_hidvbhcjfu` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8mb4, `col_gubjakxnpz` char);
ALTER TABLE `tb_mqghelxirz` ADD COLUMN (`col_hawbrmwxzw` blob(1199961922), `col_kgokeficzq` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') DEFAULT 'enum_or_set_0');
ALTER TABLE `tb_mqghelxirz` ADD (`col_dplffwllbx` char(146) CHARACTER SET utf8, `col_rikplgqsve` tinyint(146) zerofill NULL);
ALTER TABLE `tb_mqghelxirz` ADD `col_ukmsekjycj` tinyblob;
ALTER TABLE `tb_mqghelxirz` ADD COLUMN `col_ixaybtquqz` smallint(215) unsigned DEFAULT '1' FIRST;
ALTER TABLE `tb_mqghelxirz` ADD COLUMN (`col_zonuplyzdz` tinyblob, `col_snvmotcqtx` mediumtext);
ALTER TABLE `tb_mqghelxirz` CHARACTER SET = utf8mb4;
ALTER TABLE `tb_mqghelxirz` ADD CONSTRAINT symb_hewywavzus PRIMARY KEY (`col_gwwjlquinc`);
ALTER TABLE `tb_mqghelxirz` ADD UNIQUE `col_gwwjlquinc`(`col_gwwjlquinc`,`col_gubjakxnpz`);
ALTER TABLE `tb_mqghelxirz` ALTER `col_rikplgqsve` SET DEFAULT NULL;
ALTER TABLE `tb_mqghelxirz` CHANGE COLUMN `col_dplffwllbx` `col_xltpnadrcs` bigint zerofill NOT NULL;
ALTER TABLE `tb_mqghelxirz` CHANGE `col_hidvbhcjfu` `col_vtkwyvoqkp` varbinary(121) NULL;
ALTER TABLE `tb_mqghelxirz` DROP INDEX `col_gseshvltxd`;
ALTER TABLE `tb_mqghelxirz` DROP KEY `col_gwwjlquinc`;
