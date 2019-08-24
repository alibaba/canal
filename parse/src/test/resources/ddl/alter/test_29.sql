CREATE TABLE `tb_spegvrpfiz` (
  `col_zphkdkfyfk` year(4) DEFAULT '2019',
  `col_omyyogfmwf` varbinary(117),
  `col_ziqdcvhqaa` longblob,
  `col_nygklhdofu` time(3),
  UNIQUE `col_zphkdkfyfk` (`col_zphkdkfyfk`)
) DEFAULT CHARSET=utf8;
RENAME TABLE `tb_spegvrpfiz` TO `tb_lfxvmboyfq`;
ALTER TABLE `tb_lfxvmboyfq` ADD `col_ngeiottgyu` year(4);
ALTER TABLE `tb_lfxvmboyfq` ADD COLUMN `col_bvvikzmudl` bit(61);
ALTER TABLE `tb_lfxvmboyfq` ADD (`col_ozthytmepu` mediumtext CHARACTER SET utf8mb4, `col_osxbsknpgd` varchar(183) CHARACTER SET utf8mb4);
ALTER TABLE `tb_lfxvmboyfq` ADD COLUMN (`col_qkgcfgdjqf` text(932341768) CHARACTER SET utf8mb4, `col_idrqfoikzf` float(151,12) NOT NULL);
ALTER TABLE `tb_lfxvmboyfq` ADD (`col_ixxhxsglwg` bigint(92) zerofill, `col_fpciekbati` tinyint DEFAULT '1');
ALTER TABLE `tb_lfxvmboyfq` ADD `col_whrxkkdwrk` tinytext;
ALTER TABLE `tb_lfxvmboyfq` ADD COLUMN `col_ymcntbqrwg` int zerofill FIRST;
ALTER TABLE `tb_lfxvmboyfq` ADD (`col_yxfqfnhsex` bit(43), `col_vvuposukex` numeric NOT NULL DEFAULT '1');
ALTER TABLE `tb_lfxvmboyfq` ADD PRIMARY KEY (`col_idrqfoikzf`);
ALTER TABLE `tb_lfxvmboyfq` ADD UNIQUE KEY (`col_ymcntbqrwg`);
ALTER TABLE `tb_lfxvmboyfq` ALTER `col_vvuposukex` SET DEFAULT '1';
ALTER TABLE `tb_lfxvmboyfq` DROP COLUMN `col_bvvikzmudl`;
ALTER TABLE `tb_lfxvmboyfq` DROP COLUMN `col_ngeiottgyu`, DROP COLUMN `col_ymcntbqrwg`;
ALTER TABLE `tb_lfxvmboyfq` DROP `col_qkgcfgdjqf`;
ALTER TABLE `tb_lfxvmboyfq` DROP COLUMN `col_nygklhdofu`;
ALTER TABLE `tb_lfxvmboyfq` DROP COLUMN `col_fpciekbati`, DROP COLUMN `col_zphkdkfyfk`;
ALTER TABLE `tb_lfxvmboyfq` DROP `col_vvuposukex`;
ALTER TABLE `tb_lfxvmboyfq` DROP `col_ixxhxsglwg`, DROP `col_ozthytmepu`;
ALTER TABLE `tb_lfxvmboyfq` DROP PRIMARY KEY;
