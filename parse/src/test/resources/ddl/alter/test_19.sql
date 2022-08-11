CREATE TABLE `tb_gbdjlupmhj` (
  `col_zciuwofqxv` float(151,15)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `tb_yubjqntjhv` LIKE `tb_gbdjlupmhj`;
RENAME TABLE `tb_yubjqntjhv` TO `tb_obmtwwzsar`, `tb_gbdjlupmhj` TO `tb_acbpixrigq`;
RENAME TABLE `tb_obmtwwzsar` TO `tb_hefguylckk`, `tb_acbpixrigq` TO `tb_nfcvlzwedu`;
DROP TABLE tb_nfcvlzwedu;
ALTER TABLE `tb_hefguylckk` ADD `col_hjucxhsncd` mediumint(206) unsigned zerofill;
ALTER TABLE `tb_hefguylckk` ADD (`col_bblhcpxxju` tinyblob, `col_fcmfyfomtk` tinyblob);
ALTER TABLE `tb_hefguylckk` DEFAULT CHARACTER SET = utf8mb4;
ALTER TABLE `tb_hefguylckk` ADD UNIQUE `uk_qgxgwlyozf` (`col_fcmfyfomtk`(14));
ALTER TABLE `tb_hefguylckk` ADD UNIQUE INDEX (`col_bblhcpxxju`(25));
ALTER TABLE `tb_hefguylckk` CHANGE `col_zciuwofqxv` `col_oidtfumfdm` tinyblob;
ALTER TABLE `tb_hefguylckk` CHANGE `col_oidtfumfdm` `col_qzehomawzl` integer NULL;
ALTER TABLE `tb_hefguylckk` DROP COLUMN `col_fcmfyfomtk`, DROP COLUMN `col_hjucxhsncd`;
ALTER TABLE `tb_hefguylckk` DROP COLUMN `col_bblhcpxxju`;
