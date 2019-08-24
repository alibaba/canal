CREATE TABLE `tb_xuylcctytd` (
  `col_oxgdbldyfk` bigint(171) zerofill,
  `col_sfpolurvqu` double(2,2),
  `col_hkfndsohtj` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET latin1 NOT NULL,
  CONSTRAINT symb_gppmypbafa PRIMARY KEY (`col_hkfndsohtj`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
RENAME TABLE `tb_xuylcctytd` TO `tb_mvpbukrobp`;
ALTER TABLE `tb_mvpbukrobp` ADD COLUMN `col_gepoechcst` char;
ALTER TABLE `tb_mvpbukrobp` ADD COLUMN (`col_owvaejsbfe` blob, `col_hgaiklgzif` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') NULL);
ALTER TABLE `tb_mvpbukrobp` ADD (`col_ndfdyxpuhz` timestamp(0) DEFAULT CURRENT_TIMESTAMP(0), `col_hnerbhuhmd` timestamp(5) NULL DEFAULT CURRENT_TIMESTAMP(5));
ALTER TABLE `tb_mvpbukrobp` ADD (`col_vrsytgjrfw` date DEFAULT '2019-07-04', `col_vlgjlismyz` text);
ALTER TABLE `tb_mvpbukrobp` ADD (`col_vfwrbmtwyi` tinyblob, `col_qeoodzhosh` blob(445889767));
ALTER TABLE `tb_mvpbukrobp` ADD `col_ivdmwufhla` bit NULL DEFAULT b'0';
ALTER TABLE `tb_mvpbukrobp` ADD (`col_vpymqtoibu` binary(56) NOT NULL, `col_xiuoqstzzw` float(21));
ALTER TABLE `tb_mvpbukrobp` ADD COLUMN (`col_lzdzsmbdyf` tinyint(159) unsigned zerofill NULL, `col_jbqfyecqez` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') NOT NULL);
ALTER TABLE `tb_mvpbukrobp` ADD `col_fjxdpmowuy` mediumtext CHARACTER SET utf8mb4;
ALTER TABLE `tb_mvpbukrobp` ADD UNIQUE `uk_ckopkpiarf` (`col_oxgdbldyfk`,`col_sfpolurvqu`);
ALTER TABLE `tb_mvpbukrobp` ADD UNIQUE KEY `uk_nqultcfeau` (`col_ivdmwufhla`,`col_vpymqtoibu`(19));
ALTER TABLE `tb_mvpbukrobp` DROP COLUMN `col_ivdmwufhla`, DROP COLUMN `col_lzdzsmbdyf`;
ALTER TABLE `tb_mvpbukrobp` DROP `col_hnerbhuhmd`, DROP `col_sfpolurvqu`;
ALTER TABLE `tb_mvpbukrobp` DROP COLUMN `col_qeoodzhosh`;
ALTER TABLE `tb_mvpbukrobp` DROP COLUMN `col_xiuoqstzzw`;
ALTER TABLE `tb_mvpbukrobp` DROP `col_oxgdbldyfk`, DROP `col_vpymqtoibu`;
ALTER TABLE `tb_mvpbukrobp` DROP COLUMN `col_vfwrbmtwyi`;
ALTER TABLE `tb_mvpbukrobp` DROP `col_fjxdpmowuy`, DROP `col_owvaejsbfe`;
ALTER TABLE `tb_mvpbukrobp` DROP PRIMARY KEY;
