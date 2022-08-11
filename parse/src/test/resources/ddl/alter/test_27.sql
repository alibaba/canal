CREATE TABLE `tb_ffitzbfcdd` (
  `col_nfqelngymy` varchar(119) CHARACTER SET utf8 DEFAULT '',
  `col_icgtjlsqcx` year(4) NULL DEFAULT '2019',
  `col_wkerqargxk` numeric(63),
  UNIQUE `col_nfqelngymy` (`col_nfqelngymy`(6))
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
RENAME TABLE `tb_ffitzbfcdd` TO `tb_yvjscngtbt`;
RENAME TABLE `tb_yvjscngtbt` TO `tb_cnbkkspjcl`;
RENAME TABLE `tb_cnbkkspjcl` TO `tb_flnycxasap`;
ALTER TABLE `tb_flnycxasap` ADD COLUMN (`col_ctczxnvezz` timestamp(4) DEFAULT CURRENT_TIMESTAMP(4), `col_sfeoecazoa` tinyint zerofill);
ALTER TABLE `tb_flnycxasap` ADD (`col_tthopazpvp` longblob, `col_icevwmryve` mediumtext);
ALTER TABLE `tb_flnycxasap` ADD (`col_hygoqydgxc` bit, `col_rqtzrkfarg` timestamp(0) NULL DEFAULT CURRENT_TIMESTAMP(0));
ALTER TABLE `tb_flnycxasap` ADD `col_olqpjxyqbq` mediumint unsigned zerofill NOT NULL FIRST;
ALTER TABLE `tb_flnycxasap` ADD PRIMARY KEY (`col_olqpjxyqbq`);
ALTER TABLE `tb_flnycxasap` ADD UNIQUE (`col_sfeoecazoa`,`col_hygoqydgxc`);
ALTER TABLE `tb_flnycxasap` ALTER COLUMN `col_hygoqydgxc` SET DEFAULT NULL;
ALTER TABLE `tb_flnycxasap` CHANGE `col_wkerqargxk` `col_uqaobxqnuc` blob;
ALTER TABLE `tb_flnycxasap` CHANGE `col_sfeoecazoa` `col_tqseiopsqa` datetime(4) NOT NULL;
ALTER TABLE `tb_flnycxasap` CHANGE COLUMN `col_icevwmryve` `col_udrxovnqce` longtext;
ALTER TABLE `tb_flnycxasap` DROP COLUMN `col_uqaobxqnuc`;
ALTER TABLE `tb_flnycxasap` DROP `col_ctczxnvezz`, DROP `col_olqpjxyqbq`;
ALTER TABLE `tb_flnycxasap` DROP COLUMN `col_udrxovnqce`;
ALTER TABLE `tb_flnycxasap` DROP `col_tqseiopsqa`;
ALTER TABLE `tb_flnycxasap` DROP COLUMN `col_tthopazpvp`;
ALTER TABLE `tb_flnycxasap` DROP COLUMN `col_hygoqydgxc`;
ALTER TABLE `tb_flnycxasap` DROP COLUMN `col_nfqelngymy`, DROP COLUMN `col_icgtjlsqcx`;
