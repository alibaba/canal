CREATE TABLE `tb_xckrjwodah` (
  `col_pspaxkzovq` decimal,
  UNIQUE KEY `col_pspaxkzovq` (`col_pspaxkzovq`),
  UNIQUE INDEX `uk_vkwgrmlfqc` (`col_pspaxkzovq`)
) DEFAULT CHARSET=utf8;
RENAME TABLE `tb_xckrjwodah` TO `tb_ivkfbysugn`;
RENAME TABLE `tb_ivkfbysugn` TO `tb_suvmojlodk`;
RENAME TABLE `tb_suvmojlodk` TO `tb_ubziprstga`;
ALTER TABLE `tb_ubziprstga` ADD `col_bjmlsboygo` text AFTER `col_pspaxkzovq`;
ALTER TABLE `tb_ubziprstga` ADD COLUMN (`col_ayuqjzbbwg` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT 'enum_or_set_0', `col_fjhnhxlfmc` decimal(29,23));
ALTER TABLE `tb_ubziprstga` CHARACTER SET utf8mb4;
ALTER TABLE `tb_ubziprstga` CHANGE COLUMN `col_ayuqjzbbwg` `col_slcmuapykg` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8mb4 DEFAULT 'enum_or_set_0' FIRST;
ALTER TABLE `tb_ubziprstga` CHANGE `col_pspaxkzovq` `col_mlpyhxlthp` bit NOT NULL;
ALTER TABLE `tb_ubziprstga` DROP `col_mlpyhxlthp`;
ALTER TABLE `tb_ubziprstga` DROP COLUMN `col_fjhnhxlfmc`;
ALTER TABLE `tb_ubziprstga` DROP COLUMN `col_slcmuapykg`;
