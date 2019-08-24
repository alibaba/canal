CREATE TABLE `tb_yajpbebdke` (
  `col_jerxhroyff` mediumtext CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  `col_cuctvlzqvd` numeric(63,14),
  UNIQUE `col_jerxhroyff` (`col_jerxhroyff`(29)),
  UNIQUE INDEX `col_jerxhroyff_2` (`col_jerxhroyff`(15),`col_cuctvlzqvd`)
) DEFAULT CHARSET=utf8;
RENAME TABLE `tb_yajpbebdke` TO `tb_nenripsjcu`;
RENAME TABLE `tb_nenripsjcu` TO `tb_ocwsozgwid`;
ALTER TABLE `tb_ocwsozgwid` ADD UNIQUE INDEX (`col_jerxhroyff`(28));
ALTER TABLE `tb_ocwsozgwid` ADD UNIQUE KEY (`col_jerxhroyff`(19),`col_cuctvlzqvd`);
ALTER TABLE `tb_ocwsozgwid` ALTER `col_cuctvlzqvd` DROP DEFAULT;
ALTER TABLE `tb_ocwsozgwid` DROP COLUMN `col_jerxhroyff`;
