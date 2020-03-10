CREATE TABLE `tb_ybomzxquze` (
  `col_xjawafguvi` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8mb4 NULL,
  `col_juwaxfpier` longblob,
  `col_xnhktpwgoy` tinyblob,
  `col_sfflidbosv` timestamp(2) NULL,
  UNIQUE INDEX `col_xjawafguvi` (`col_xjawafguvi`),
  UNIQUE `uk_tlmnsowqus` (`col_sfflidbosv`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
RENAME TABLE `tb_ybomzxquze` TO `tb_cugzkpefkt`;
ALTER TABLE `tb_cugzkpefkt` ADD (`col_vmjyaetcdt` text(3199621179), `col_qstgsvuago` mediumtext CHARACTER SET utf8mb4);
ALTER TABLE `tb_cugzkpefkt` ADD (`col_noqqlolqsk` blob(1755778528), `col_gyittdjhfg` numeric);
ALTER TABLE `tb_cugzkpefkt` CHARACTER SET utf8;
ALTER TABLE `tb_cugzkpefkt` ADD UNIQUE KEY `uk_anpzhhtomg` (`col_xjawafguvi`,`col_xnhktpwgoy`(16));
ALTER TABLE `tb_cugzkpefkt` ADD UNIQUE INDEX (`col_vmjyaetcdt`(29),`col_qstgsvuago`(1));
ALTER TABLE `tb_cugzkpefkt` DROP `col_juwaxfpier`, DROP `col_qstgsvuago`;
ALTER TABLE `tb_cugzkpefkt` DROP `col_xjawafguvi`;
ALTER TABLE `tb_cugzkpefkt` DROP `col_sfflidbosv`;
ALTER TABLE `tb_cugzkpefkt` DROP COLUMN `col_gyittdjhfg`;
ALTER TABLE `tb_cugzkpefkt` DROP COLUMN `col_vmjyaetcdt`;
ALTER TABLE `tb_cugzkpefkt` DROP `col_noqqlolqsk`;
