CREATE TABLE `tb_bfwanxinyr` (
  `col_msdkdkrbqj` tinytext CHARACTER SET latin1,
  `col_qkgtxzzfie` tinyblob,
  UNIQUE INDEX `col_msdkdkrbqj` (`col_msdkdkrbqj`(30))
) DEFAULT CHARSET=latin1;
RENAME TABLE `tb_bfwanxinyr` TO `tb_wvhiikgeky`;
RENAME TABLE `tb_wvhiikgeky` TO `tb_krsikytrbw`;
ALTER TABLE `tb_krsikytrbw` ADD COLUMN (`col_lfbganjgad` timestamp(4) NULL, `col_lyscxymhbl` integer unsigned DEFAULT '1');
ALTER TABLE `tb_krsikytrbw` ADD (`col_eystskhwfm` char CHARACTER SET utf8mb4, `col_cqjptjcgyb` binary NOT NULL);
ALTER TABLE `tb_krsikytrbw` ADD COLUMN (`col_mlvshwiulp` numeric(15), `col_lfptlilmsv` int(230) zerofill NOT NULL);
ALTER TABLE `tb_krsikytrbw` ADD COLUMN (`col_inukcthnws` varchar(114), `col_omxbpiulnd` year DEFAULT '2019');
ALTER TABLE `tb_krsikytrbw` ADD COLUMN `col_lnpeucimig` time NOT NULL DEFAULT '00:00:00';
ALTER TABLE `tb_krsikytrbw` ADD `col_idovrewllo` tinytext FIRST;
ALTER TABLE `tb_krsikytrbw` ADD `col_szvdshblvc` tinyint(109) unsigned zerofill;
ALTER TABLE `tb_krsikytrbw` ADD COLUMN `col_qgtkapnaff` blob(2990008429);
ALTER TABLE `tb_krsikytrbw` ADD CONSTRAINT symb_kcuznajfjp PRIMARY KEY (`col_cqjptjcgyb`);
ALTER TABLE `tb_krsikytrbw` ADD UNIQUE (`col_qgtkapnaff`(16));
ALTER TABLE `tb_krsikytrbw` ADD UNIQUE INDEX (`col_cqjptjcgyb`,`col_szvdshblvc`);
ALTER TABLE `tb_krsikytrbw` ALTER `col_eystskhwfm` DROP DEFAULT;
ALTER TABLE `tb_krsikytrbw` ALTER COLUMN `col_cqjptjcgyb` DROP DEFAULT;
ALTER TABLE `tb_krsikytrbw` ALTER COLUMN `col_lnpeucimig` SET DEFAULT '00:00:00';
ALTER TABLE `tb_krsikytrbw` CHANGE `col_idovrewllo` `col_lkhsufhjwr` mediumint(8) zerofill NULL;
ALTER TABLE `tb_krsikytrbw` CHANGE COLUMN `col_lnpeucimig` `col_zdneomelnh` decimal(14) NOT NULL;
ALTER TABLE `tb_krsikytrbw` CHANGE `col_szvdshblvc` `col_lliihnoyoo` integer(145) NOT NULL DEFAULT '1' AFTER `col_lkhsufhjwr`;
ALTER TABLE `tb_krsikytrbw` DROP `col_mlvshwiulp`, DROP `col_lyscxymhbl`;
ALTER TABLE `tb_krsikytrbw` DROP COLUMN `col_inukcthnws`;
ALTER TABLE `tb_krsikytrbw` DROP `col_qgtkapnaff`;
ALTER TABLE `tb_krsikytrbw` DROP COLUMN `col_lkhsufhjwr`, DROP COLUMN `col_msdkdkrbqj`;
ALTER TABLE `tb_krsikytrbw` DROP COLUMN `col_eystskhwfm`, DROP COLUMN `col_zdneomelnh`;
