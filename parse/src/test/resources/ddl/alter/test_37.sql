CREATE TABLE `tb_qxifuytosb` (
  `col_xxcylzlczf` blob(2724000156),
  `col_sanwlcnfin` time NULL,
  `col_qxmjojclfy` bit(56) NULL,
  `col_uzjeazqmyh` mediumblob,
  UNIQUE `col_xxcylzlczf` (`col_xxcylzlczf`(10)),
  UNIQUE `col_xxcylzlczf_2` (`col_xxcylzlczf`(4),`col_uzjeazqmyh`(4))
) DEFAULT CHARSET=latin1;
CREATE TABLE `tb_jiijqyzrrm` (
  `col_xmaniyllht` year(4) NULL,
  UNIQUE KEY `col_xmaniyllht` (`col_xmaniyllht`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
RENAME TABLE `tb_qxifuytosb` TO `tb_pxjfszwfyt`;
DROP TABLE tb_jiijqyzrrm, tb_pxjfszwfyt;
CREATE TABLE `tb_cdbrmxbknb` (
  `col_rsqdthkvva` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8 NULL,
  `col_amvugezhpd` blob,
  UNIQUE INDEX `uk_yvoxkgeget` (`col_amvugezhpd`(6)),
  UNIQUE `uk_ozxqwxureo` (`col_rsqdthkvva`,`col_amvugezhpd`(10))
) DEFAULT CHARSET=latin1;
ALTER TABLE `tb_cdbrmxbknb` ADD COLUMN (`col_cybpuhkzzb` datetime(6) NULL, `col_fssjphbkbw` mediumtext CHARACTER SET utf8 COLLATE utf8_unicode_ci);
ALTER TABLE `tb_cdbrmxbknb` DEFAULT CHARACTER SET = utf8;
ALTER TABLE `tb_cdbrmxbknb` ADD UNIQUE (`col_fssjphbkbw`(1));
ALTER TABLE `tb_cdbrmxbknb` ALTER `col_cybpuhkzzb` DROP DEFAULT;
ALTER TABLE `tb_cdbrmxbknb` CHANGE COLUMN `col_cybpuhkzzb` `col_lxbevqcyyv` bigint(33) zerofill NULL FIRST;
ALTER TABLE `tb_cdbrmxbknb` CHANGE COLUMN `col_fssjphbkbw` `col_vobywwqdrd` datetime DEFAULT '2019-07-04 00:00:00';
ALTER TABLE `tb_cdbrmxbknb` DROP COLUMN `col_lxbevqcyyv`, DROP COLUMN `col_vobywwqdrd`;
ALTER TABLE `tb_cdbrmxbknb` DROP `col_rsqdthkvva`;
