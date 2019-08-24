CREATE TABLE `tb_xfewelqveu` (
  `col_vzrjzlcvms` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  `col_sdyhbfaqdm` tinytext CHARACTER SET utf8,
  `col_teupgphirm` numeric,
  UNIQUE INDEX `col_sdyhbfaqdm` (`col_sdyhbfaqdm`(17)),
  UNIQUE `col_sdyhbfaqdm_2` (`col_sdyhbfaqdm`(2))
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
RENAME TABLE `tb_xfewelqveu` TO `tb_upgbkyyruv`;
RENAME TABLE `tb_upgbkyyruv` TO `tb_ahefjpyxqu`;
ALTER TABLE `tb_ahefjpyxqu` ADD (`col_nkknhggvii` bigint(2) unsigned, `col_egwbfvawdd` double(243,27));
ALTER TABLE `tb_ahefjpyxqu` ALTER COLUMN `col_egwbfvawdd` DROP DEFAULT;
ALTER TABLE `tb_ahefjpyxqu` CHANGE COLUMN `col_teupgphirm` `col_cqrrpavzlu` tinyint(14) NULL DEFAULT '1' AFTER `col_sdyhbfaqdm`;
ALTER TABLE `tb_ahefjpyxqu` CHANGE `col_vzrjzlcvms` `col_djbztucmiv` datetime NULL DEFAULT '2019-07-04 00:00:00' FIRST;
ALTER TABLE `tb_ahefjpyxqu` DROP COLUMN `col_sdyhbfaqdm`, DROP COLUMN `col_djbztucmiv`;
ALTER TABLE `tb_ahefjpyxqu` DROP COLUMN `col_egwbfvawdd`, DROP COLUMN `col_nkknhggvii`;
