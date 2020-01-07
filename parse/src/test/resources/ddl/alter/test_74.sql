CREATE TABLE `tb_ukcldpswkg` (
  `col_slkmwynegf` integer(250) unsigned zerofill NULL,
  `col_cjsmvrdiur` bit,
  `col_prxndrinom` integer unsigned DEFAULT '1',
  `col_kihpmvmflx` double,
  UNIQUE `col_prxndrinom` (`col_prxndrinom`),
  UNIQUE `col_slkmwynegf` (`col_slkmwynegf`,`col_cjsmvrdiur`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `tb_rxvmngvuko` (
  `col_vabvvfkcss` integer unsigned NULL,
  `col_sedpeihgba` bit DEFAULT b'0',
  `col_pjbwfuvwmj` binary(254) NULL,
  UNIQUE `uk_oqxlgofciy` (`col_pjbwfuvwmj`(31))
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
RENAME TABLE `tb_ukcldpswkg` TO `tb_kifaamclft`;
RENAME TABLE `tb_rxvmngvuko` TO `tb_jfcolfofka`;
DROP TABLE tb_jfcolfofka, tb_kifaamclft;
CREATE TABLE `tb_tazkjnlgrx` (
  `col_tbjrxivrjg` blob
) DEFAULT CHARSET=utf8;
ALTER TABLE `tb_tazkjnlgrx` ADD (`col_fuqpghhcyy` float, `col_sbwclfupup` binary NULL);
ALTER TABLE `tb_tazkjnlgrx` ADD COLUMN (`col_bmyrokxows` time(4), `col_jxosxbunsc` double(179,7) NULL);
ALTER TABLE `tb_tazkjnlgrx` ADD COLUMN `col_trhadxfbfi` numeric FIRST;
ALTER TABLE `tb_tazkjnlgrx` ADD COLUMN `col_jijmncnsoa` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') DEFAULT 'enum_or_set_0';
ALTER TABLE `tb_tazkjnlgrx` ADD COLUMN `col_ucszyswhkc` bigint zerofill;
ALTER TABLE `tb_tazkjnlgrx` ADD COLUMN `col_fbxbftvxcl` smallint(14) zerofill;
ALTER TABLE `tb_tazkjnlgrx` ADD (`col_vbsyfqxqng` mediumblob, `col_ktnctyhcvl` blob);
ALTER TABLE `tb_tazkjnlgrx` ADD COLUMN `col_pbgxpvfusq` tinyblob AFTER `col_fbxbftvxcl`;
ALTER TABLE `tb_tazkjnlgrx` ADD COLUMN `col_ewzdxvnlrs` bit NOT NULL DEFAULT b'0' FIRST;
ALTER TABLE `tb_tazkjnlgrx` ADD PRIMARY KEY (`col_ewzdxvnlrs`);
ALTER TABLE `tb_tazkjnlgrx` ADD UNIQUE KEY `uk_ygarqhlsva` (`col_tbjrxivrjg`(5),`col_jxosxbunsc`);
ALTER TABLE `tb_tazkjnlgrx` CHANGE `col_pbgxpvfusq` `col_pgtqyvckbi` time;
ALTER TABLE `tb_tazkjnlgrx` CHANGE `col_bmyrokxows` `col_gxpgglefyh` char(40) CHARACTER SET utf8mb4 NULL AFTER `col_ucszyswhkc`;
ALTER TABLE `tb_tazkjnlgrx` DROP COLUMN `col_ucszyswhkc`, DROP COLUMN `col_fuqpghhcyy`;
ALTER TABLE `tb_tazkjnlgrx` DROP `col_ktnctyhcvl`, DROP `col_fbxbftvxcl`;
ALTER TABLE `tb_tazkjnlgrx` DROP COLUMN `col_tbjrxivrjg`;
ALTER TABLE `tb_tazkjnlgrx` DROP COLUMN `col_sbwclfupup`;
