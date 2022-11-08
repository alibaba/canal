CREATE TABLE `tb_sxbbqclsgf` (
  `col_avzjcdfkfv` year(4) NOT NULL DEFAULT '2019',
  `col_kgfprqgdwt` mediumtext CHARACTER SET latin1,
  CONSTRAINT PRIMARY KEY (`col_avzjcdfkfv`),
  UNIQUE `col_kgfprqgdwt` (`col_kgfprqgdwt`(6))
) DEFAULT CHARSET=latin1;
CREATE TABLE `tb_uakpdqzysm` (
  `col_zknooyoueg` time NOT NULL DEFAULT '00:00:00',
  `col_xavytdfgxu` mediumtext CHARACTER SET utf8,
  `col_mgwiohebft` mediumtext CHARACTER SET utf8,
  `col_bejzwcvfmz` date DEFAULT '2019-07-04',
  UNIQUE `uk_cryfvwxbvx` (`col_zknooyoueg`,`col_xavytdfgxu`(9))
) DEFAULT CHARSET=utf8;
CREATE TABLE `tb_vktpftxscc` (
  `col_wqtdmdoyoo` longblob
) DEFAULT CHARSET=latin1;
RENAME TABLE `tb_uakpdqzysm` TO `tb_btqvbuxdyv`, `tb_vktpftxscc` TO `tb_fficunfgca`;
DROP TABLE tb_fficunfgca;
ALTER TABLE `tb_sxbbqclsgf` ADD `col_glgspzmrxu` mediumint zerofill NOT NULL AFTER `col_avzjcdfkfv`;
ALTER TABLE `tb_sxbbqclsgf` ADD COLUMN (`col_rezszlbpen` mediumtext, `col_nmxcrltjbv` binary);
ALTER TABLE `tb_sxbbqclsgf` ADD (`col_wctphwmxin` smallint(103) unsigned zerofill NULL, `col_yadwowaswm` mediumint(118) unsigned zerofill NOT NULL);
ALTER TABLE `tb_sxbbqclsgf` ADD COLUMN `col_tekylcrmef` bit(17) NULL DEFAULT b'0';
ALTER TABLE `tb_sxbbqclsgf` ADD COLUMN `col_ukcpwepvny` timestamp(1) DEFAULT CURRENT_TIMESTAMP(1) FIRST;
ALTER TABLE `tb_sxbbqclsgf` DEFAULT CHARACTER SET utf8;
ALTER TABLE `tb_sxbbqclsgf` ADD UNIQUE `uk_htumixicdh` (`col_avzjcdfkfv`,`col_kgfprqgdwt`(28));
ALTER TABLE `tb_sxbbqclsgf` ALTER COLUMN `col_nmxcrltjbv` SET DEFAULT NULL;
ALTER TABLE `tb_sxbbqclsgf` CHANGE `col_yadwowaswm` `col_tappjfodgm` date NULL;
ALTER TABLE `tb_sxbbqclsgf` CHANGE COLUMN `col_nmxcrltjbv` `col_raprmikjbo` date;
ALTER TABLE `tb_sxbbqclsgf` DROP COLUMN `col_tappjfodgm`, DROP COLUMN `col_glgspzmrxu`;
ALTER TABLE `tb_sxbbqclsgf` DROP COLUMN `col_rezszlbpen`, DROP COLUMN `col_raprmikjbo`;
ALTER TABLE `tb_sxbbqclsgf` DROP PRIMARY KEY;
ALTER TABLE `tb_sxbbqclsgf` DROP KEY `uk_htumixicdh`;
