CREATE TABLE `tb_yntgfjruqk` (
  `col_bxnkhgjjdz` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8mb4 DEFAULT 'enum_or_set_0',
  `col_rwmjknctzl` datetime(6) NOT NULL,
  `col_seknewafqd` timestamp,
  PRIMARY KEY (`col_rwmjknctzl`)
) DEFAULT CHARSET=latin1;
CREATE TABLE `tb_ajxoovpkvw` (
  `col_ytfnmhhaoa` int unsigned,
  `col_vimtlabyrl` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8mb4 NULL,
  UNIQUE `col_ytfnmhhaoa` (`col_ytfnmhhaoa`),
  UNIQUE KEY `col_vimtlabyrl` (`col_vimtlabyrl`)
) DEFAULT CHARSET=utf8;
RENAME TABLE `tb_ajxoovpkvw` TO `tb_qlvbveqzna`, `tb_yntgfjruqk` TO `tb_eegcnsbywv`;
ALTER TABLE `tb_eegcnsbywv` ADD (`col_jophiowutg` longtext, `col_pnotecfjsi` decimal);
ALTER TABLE `tb_eegcnsbywv` ADD `col_eawvokgzvi` year(4) NOT NULL DEFAULT '2019';
ALTER TABLE `tb_eegcnsbywv` ADD `col_orlvhbnaer` longtext CHARACTER SET utf8mb4;
ALTER TABLE `tb_eegcnsbywv` ADD COLUMN (`col_krlfmduttq` longtext, `col_jobdxewixt` bigint(173));
ALTER TABLE `tb_eegcnsbywv` ADD (`col_ufdhvspoka` float DEFAULT '1', `col_qqdcbaucve` date);
ALTER TABLE `tb_eegcnsbywv` ADD COLUMN (`col_uuexgtwrse` double(172,2) NOT NULL, `col_bfsjjimztp` datetime(1) NOT NULL);
ALTER TABLE `tb_eegcnsbywv` ADD `col_trrvagfhxb` longblob AFTER `col_uuexgtwrse`;
ALTER TABLE `tb_eegcnsbywv` ADD (`col_hfrmqomgva` date, `col_mepqygbcxa` text CHARACTER SET utf8mb4);
ALTER TABLE `tb_eegcnsbywv` ADD `col_xmubzkmxzy` mediumblob;
ALTER TABLE `tb_eegcnsbywv` DEFAULT CHARACTER SET = utf8;
ALTER TABLE `tb_eegcnsbywv` ADD UNIQUE INDEX `col_jophiowutg`(`col_jophiowutg`(2),`col_pnotecfjsi`);
ALTER TABLE `tb_eegcnsbywv` ADD UNIQUE `col_trrvagfhxb`(`col_trrvagfhxb`(20),`col_hfrmqomgva`);
ALTER TABLE `tb_eegcnsbywv` ALTER COLUMN `col_eawvokgzvi` DROP DEFAULT;
ALTER TABLE `tb_eegcnsbywv` CHANGE COLUMN `col_bxnkhgjjdz` `col_iedptbbtmp` float(41) AFTER `col_jophiowutg`;
ALTER TABLE `tb_eegcnsbywv` CHANGE `col_trrvagfhxb` `col_hzixmjzrzg` tinyint(195) unsigned zerofill NOT NULL;
ALTER TABLE `tb_eegcnsbywv` DROP INDEX `col_jophiowutg`;
ALTER TABLE `tb_eegcnsbywv` DROP KEY `col_trrvagfhxb`;
