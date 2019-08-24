CREATE TABLE `tb_fwnltgnpeg` (
  `col_gzxmlukzgv` smallint(169) unsigned zerofill NOT NULL,
  PRIMARY KEY (`col_gzxmlukzgv`),
  UNIQUE INDEX `uk_gwlvlnvrkh` (`col_gzxmlukzgv`),
  UNIQUE INDEX `uk_ygsnwxzroc` (`col_gzxmlukzgv`)
) DEFAULT CHARSET=utf8;
RENAME TABLE `tb_fwnltgnpeg` TO `tb_hmfoniytbf`;
ALTER TABLE `tb_hmfoniytbf` ADD COLUMN `col_gxqvwbkvyi` datetime(5);
ALTER TABLE `tb_hmfoniytbf` ADD COLUMN (`col_fthqvynbuq` mediumint unsigned zerofill, `col_dcxnatwddd` date DEFAULT '2019-07-04');
ALTER TABLE `tb_hmfoniytbf` ADD COLUMN `col_wvbemkrzdt` longtext CHARACTER SET utf8 COLLATE utf8_unicode_ci FIRST;
ALTER TABLE `tb_hmfoniytbf` ADD `col_alzezpawqi` longblob;
ALTER TABLE `tb_hmfoniytbf` ADD COLUMN `col_tpzhsbkmka` longtext;
ALTER TABLE `tb_hmfoniytbf` ADD COLUMN (`col_urfoabrjor` bit(15) NOT NULL, `col_xreorfbhxc` decimal(35) NULL);
ALTER TABLE `tb_hmfoniytbf` DEFAULT CHARACTER SET utf8;
ALTER TABLE `tb_hmfoniytbf` ADD UNIQUE INDEX `uk_ztdbxlchxi` (`col_alzezpawqi`(18),`col_tpzhsbkmka`(30));
ALTER TABLE `tb_hmfoniytbf` ALTER COLUMN `col_dcxnatwddd` DROP DEFAULT;
ALTER TABLE `tb_hmfoniytbf` DROP COLUMN `col_tpzhsbkmka`, DROP COLUMN `col_dcxnatwddd`;
ALTER TABLE `tb_hmfoniytbf` DROP `col_alzezpawqi`, DROP `col_wvbemkrzdt`;
ALTER TABLE `tb_hmfoniytbf` DROP `col_gzxmlukzgv`, DROP `col_xreorfbhxc`;
ALTER TABLE `tb_hmfoniytbf` DROP `col_gxqvwbkvyi`, DROP `col_fthqvynbuq`;
