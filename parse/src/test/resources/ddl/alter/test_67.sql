CREATE TABLE `tb_mfgvnrsvmu` (
  `col_bmbjupfwey` longblob,
  `col_zlboqlzvrz` binary(0) NOT NULL,
  `col_nxhonchgtt` bit NOT NULL,
  `col_wituskplcw` year(4) DEFAULT '2019',
  PRIMARY KEY (`col_nxhonchgtt`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
RENAME TABLE `tb_mfgvnrsvmu` TO `tb_psaessavwh`;
RENAME TABLE `tb_psaessavwh` TO `tb_pgqtfhwbzc`;
RENAME TABLE `tb_pgqtfhwbzc` TO `tb_wzvjzesyqu`;
ALTER TABLE `tb_wzvjzesyqu` ADD `col_gsgnhbocxa` date NOT NULL DEFAULT '2019-07-04' FIRST;
ALTER TABLE `tb_wzvjzesyqu` ADD `col_buaiwekzno` float DEFAULT '1' FIRST;
ALTER TABLE `tb_wzvjzesyqu` DEFAULT CHARACTER SET = utf8;
ALTER TABLE `tb_wzvjzesyqu` ALTER `col_nxhonchgtt` DROP DEFAULT;
ALTER TABLE `tb_wzvjzesyqu` ALTER COLUMN `col_gsgnhbocxa` DROP DEFAULT;
ALTER TABLE `tb_wzvjzesyqu` ALTER COLUMN `col_gsgnhbocxa` DROP DEFAULT;
ALTER TABLE `tb_wzvjzesyqu` DROP `col_bmbjupfwey`;
ALTER TABLE `tb_wzvjzesyqu` DROP COLUMN `col_gsgnhbocxa`, DROP COLUMN `col_wituskplcw`;
ALTER TABLE `tb_wzvjzesyqu` DROP `col_nxhonchgtt`, DROP `col_buaiwekzno`;
