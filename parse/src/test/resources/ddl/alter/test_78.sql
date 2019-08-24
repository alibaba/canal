CREATE TABLE `tb_fbpivvptsa` (
  `col_hbauqbdefn` datetime,
  UNIQUE KEY `uk_cxmeeaqnza` (`col_hbauqbdefn`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
RENAME TABLE `tb_fbpivvptsa` TO `tb_rtvtzkfcfx`;
RENAME TABLE `tb_rtvtzkfcfx` TO `tb_cockfzvkou`;
ALTER TABLE `tb_cockfzvkou` ADD `col_unrdwzfczo` tinyblob AFTER `col_hbauqbdefn`;
ALTER TABLE `tb_cockfzvkou` CHARACTER SET = utf8;
ALTER TABLE `tb_cockfzvkou` ADD UNIQUE (`col_hbauqbdefn`,`col_unrdwzfczo`(7));
ALTER TABLE `tb_cockfzvkou` ADD UNIQUE (`col_hbauqbdefn`);
ALTER TABLE `tb_cockfzvkou` CHANGE COLUMN `col_hbauqbdefn` `col_atkczpjiqk` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') NOT NULL;
ALTER TABLE `tb_cockfzvkou` DROP `col_unrdwzfczo`;
ALTER TABLE `tb_cockfzvkou` DROP KEY `col_hbauqbdefn_2`;
ALTER TABLE `tb_cockfzvkou` DROP INDEX `col_hbauqbdefn`;
