CREATE TABLE `tb_owgbdhiwmn` (
  `col_nhjtnryfsu` decimal(13,0),
  `col_dmrqgaovxi` longblob,
  `col_gvwexllvhe` decimal(35) NULL
) DEFAULT CHARSET=utf8;
CREATE TABLE `tb_xlwzxxkunf` LIKE `tb_owgbdhiwmn`;
CREATE TABLE `tb_aidhgjhqzp` (
  `col_ouqqfnoijf` tinyint(142) unsigned DEFAULT '1',
  `col_wzzrzavmqu` int unsigned zerofill,
  UNIQUE KEY `col_ouqqfnoijf` (`col_ouqqfnoijf`)
) DEFAULT CHARSET=latin1;
RENAME TABLE `tb_owgbdhiwmn` TO `tb_vpcgqeykeh`, `tb_xlwzxxkunf` TO `tb_xyniefhewz`;
DROP TABLE tb_vpcgqeykeh, tb_xyniefhewz;
ALTER TABLE `tb_aidhgjhqzp` ADD COLUMN (`col_cvjkhkkerl` datetime(1) NULL, `col_cvzuqkfnnp` tinytext);
ALTER TABLE `tb_aidhgjhqzp` ADD COLUMN (`col_dpnzvoemce` smallint(146) zerofill, `col_wisgyhivgt` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8mb4 DEFAULT 'enum_or_set_0');
ALTER TABLE `tb_aidhgjhqzp` ADD COLUMN `col_jsjfylwbsu` time(3) NOT NULL AFTER `col_ouqqfnoijf`;
ALTER TABLE `tb_aidhgjhqzp` CHARACTER SET = utf8mb4;
ALTER TABLE `tb_aidhgjhqzp` CHANGE `col_wzzrzavmqu` `col_wcosqedhbh` tinytext CHARACTER SET utf8mb4 AFTER `col_cvjkhkkerl`;
ALTER TABLE `tb_aidhgjhqzp` CHANGE COLUMN `col_cvjkhkkerl` `col_zcmworbhql` double NOT NULL FIRST;
ALTER TABLE `tb_aidhgjhqzp` CHANGE COLUMN `col_zcmworbhql` `col_wtsgpcyuec` time(6) NULL FIRST;
ALTER TABLE `tb_aidhgjhqzp` DROP INDEX `col_ouqqfnoijf`;
