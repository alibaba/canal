CREATE TABLE `tb_zsvasnztbf` (
  `col_vqskztkwqb` blob(2179715620),
  `col_xrutymggiq` text CHARACTER SET latin1,
  `col_rjrhithgjd` varbinary(93) NOT NULL,
  PRIMARY KEY (`col_rjrhithgjd`(23)),
  UNIQUE `uk_qcuhitxxlz` (`col_rjrhithgjd`(12))
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE TABLE `tb_joibqmiluh` (
  `col_aszmqpjfka` numeric(49),
  `col_qmwcrinzvf` time NOT NULL,
  `col_xgwavsjbcg` longblob,
  UNIQUE `col_xgwavsjbcg` (`col_xgwavsjbcg`(22))
) DEFAULT CHARSET=utf8;
RENAME TABLE `tb_zsvasnztbf` TO `tb_sxhjnreyoa`, `tb_joibqmiluh` TO `tb_qlhhbdmoeq`;
RENAME TABLE `tb_sxhjnreyoa` TO `tb_idttivasrc`, `tb_qlhhbdmoeq` TO `tb_rjdimmicei`;
ALTER TABLE `tb_idttivasrc` ADD (`col_qgehaatedk` tinyint unsigned zerofill NOT NULL, `col_ngqfbkxgkw` tinyblob);
ALTER TABLE `tb_idttivasrc` ADD `col_naimzyvtyy` mediumint unsigned AFTER `col_qgehaatedk`;
ALTER TABLE `tb_idttivasrc` ADD COLUMN `col_ybyyrfaujt` longtext;
ALTER TABLE `tb_idttivasrc` ADD COLUMN (`col_slxxarfdmi` binary(243), `col_fueiigtecm` blob(2615853032));
ALTER TABLE `tb_idttivasrc` DEFAULT CHARACTER SET utf8mb4;
ALTER TABLE `tb_idttivasrc` ALTER COLUMN `col_rjrhithgjd` DROP DEFAULT;
ALTER TABLE `tb_idttivasrc` DROP COLUMN `col_rjrhithgjd`;
ALTER TABLE `tb_idttivasrc` DROP COLUMN `col_fueiigtecm`, DROP COLUMN `col_ybyyrfaujt`;
ALTER TABLE `tb_idttivasrc` DROP `col_qgehaatedk`, DROP `col_xrutymggiq`;
