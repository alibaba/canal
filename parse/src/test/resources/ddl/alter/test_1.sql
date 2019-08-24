CREATE TABLE `tb_ijlfoushao` (
  `col_ldgkfndhbx` tinytext CHARACTER SET utf8,
  `col_bgzuestpsr` bit(47) DEFAULT b'0'
) DEFAULT CHARSET=utf8;
RENAME TABLE `tb_ijlfoushao` TO `tb_fmmsfdptfj`;
RENAME TABLE `tb_fmmsfdptfj` TO `tb_bthnyesbdu`;
RENAME TABLE `tb_bthnyesbdu` TO `tb_rpwzfoyxgb`;
ALTER TABLE `tb_rpwzfoyxgb` ADD COLUMN `col_mpjirrklef` text(161554088) CHARACTER SET utf8mb4;
ALTER TABLE `tb_rpwzfoyxgb` ADD COLUMN `col_ymhswmzvva` time NULL FIRST;
ALTER TABLE `tb_rpwzfoyxgb` ADD `col_hxpgdeuejv` blob;
ALTER TABLE `tb_rpwzfoyxgb` ADD (`col_uuvxfsqeat` tinyint NOT NULL DEFAULT '1', `col_hvwlhidvlq` int(166) NULL);
ALTER TABLE `tb_rpwzfoyxgb` ADD CONSTRAINT PRIMARY KEY (`col_uuvxfsqeat`);
ALTER TABLE `tb_rpwzfoyxgb` CHANGE COLUMN `col_ymhswmzvva` `col_cuhqbcsmob` smallint(198) unsigned DEFAULT '1';
ALTER TABLE `tb_rpwzfoyxgb` DROP COLUMN `col_bgzuestpsr`, DROP COLUMN `col_hxpgdeuejv`;
ALTER TABLE `tb_rpwzfoyxgb` DROP `col_cuhqbcsmob`, DROP `col_hvwlhidvlq`;
ALTER TABLE `tb_rpwzfoyxgb` DROP COLUMN `col_uuvxfsqeat`, DROP COLUMN `col_ldgkfndhbx`;
