CREATE TABLE `tb_edzrjofyjy` (
  `col_ryuwsvbvzl` longblob,
  `col_hqgbphjulh` timestamp
) DEFAULT CHARSET=utf8;
RENAME TABLE `tb_edzrjofyjy` TO `tb_dhnyjpcqwn`;
RENAME TABLE `tb_dhnyjpcqwn` TO `tb_pdshedxikd`;
RENAME TABLE `tb_pdshedxikd` TO `tb_nuprsugtcj`;
ALTER TABLE `tb_nuprsugtcj` ADD `col_ezppromjmx` int(120) unsigned zerofill NULL;
ALTER TABLE `tb_nuprsugtcj` ADD COLUMN (`col_qwsjkklpfs` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8mb4 NULL DEFAULT 'enum_or_set_0', `col_iyhhfsuidc` double);
ALTER TABLE `tb_nuprsugtcj` DEFAULT CHARACTER SET utf8mb4;
ALTER TABLE `tb_nuprsugtcj` ADD UNIQUE KEY (`col_iyhhfsuidc`);
ALTER TABLE `tb_nuprsugtcj` ADD UNIQUE KEY `uk_mmyhqwqrpg` (`col_ezppromjmx`);
ALTER TABLE `tb_nuprsugtcj` ALTER COLUMN `col_qwsjkklpfs` SET DEFAULT NULL;
ALTER TABLE `tb_nuprsugtcj` ALTER COLUMN `col_qwsjkklpfs` DROP DEFAULT;
ALTER TABLE `tb_nuprsugtcj` CHANGE COLUMN `col_ryuwsvbvzl` `col_yxvzojfrof` smallint(39) DEFAULT '1' FIRST;
ALTER TABLE `tb_nuprsugtcj` DROP COLUMN `col_iyhhfsuidc`, DROP COLUMN `col_hqgbphjulh`;
ALTER TABLE `tb_nuprsugtcj` DROP COLUMN `col_yxvzojfrof`, DROP COLUMN `col_ezppromjmx`;
