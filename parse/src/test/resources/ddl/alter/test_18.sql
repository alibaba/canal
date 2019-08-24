CREATE TABLE `tb_pnracjzmkp` (
  `col_zoovnitdls` float(13,13),
  `col_prrcfbnwew` longtext CHARACTER SET utf8mb4,
  `col_peqguxwzcy` smallint(202) NOT NULL DEFAULT '1'
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `tb_qwbjzchejk` LIKE `tb_pnracjzmkp`;
RENAME TABLE `tb_qwbjzchejk` TO `tb_ezxnrjkvwu`, `tb_pnracjzmkp` TO `tb_jhkrixchju`;
RENAME TABLE `tb_ezxnrjkvwu` TO `tb_nxqxynvjvx`;
DROP TABLE tb_nxqxynvjvx;
ALTER TABLE `tb_jhkrixchju` ADD COLUMN (`col_fpmvnrnoxw` varchar(18) NOT NULL, `col_hrcqpnxnrn` text(1490675533));
ALTER TABLE `tb_jhkrixchju` ADD COLUMN (`col_iguvbfennm` year(4) NOT NULL, `col_nhdlxtgtvs` char NULL);
ALTER TABLE `tb_jhkrixchju` ADD `col_vyizooodsa` longblob AFTER `col_nhdlxtgtvs`;
ALTER TABLE `tb_jhkrixchju` ADD (`col_hwmrzghead` longtext CHARACTER SET utf8mb4, `col_adgnavqmhq` longtext CHARACTER SET utf8mb4);
ALTER TABLE `tb_jhkrixchju` ADD (`col_adwetrnlyu` bit(41), `col_ypyzwlctxm` varbinary(86) DEFAULT '\0');
ALTER TABLE `tb_jhkrixchju` ADD (`col_lkdcwbjkis` varbinary(61), `col_rmjdxsrzry` char(159) CHARACTER SET utf8mb4);
ALTER TABLE `tb_jhkrixchju` ADD (`col_sfmloqjxwi` decimal(33,29) NOT NULL, `col_mcjrxquwtj` binary);
ALTER TABLE `tb_jhkrixchju` ADD COLUMN (`col_fxxexsgvmq` int(11) unsigned NOT NULL, `col_ilcapoaext` mediumblob);
ALTER TABLE `tb_jhkrixchju` ADD (`col_dvcgmijmcd` smallint(246) unsigned NOT NULL DEFAULT '1', `col_avjufpxxft` timestamp NULL);
ALTER TABLE `tb_jhkrixchju` DEFAULT CHARACTER SET = utf8mb4;
ALTER TABLE `tb_jhkrixchju` DROP `col_fxxexsgvmq`, DROP `col_iguvbfennm`;
ALTER TABLE `tb_jhkrixchju` DROP `col_zoovnitdls`, DROP `col_lkdcwbjkis`;
ALTER TABLE `tb_jhkrixchju` DROP COLUMN `col_fpmvnrnoxw`;
ALTER TABLE `tb_jhkrixchju` DROP `col_sfmloqjxwi`, DROP `col_hrcqpnxnrn`;
