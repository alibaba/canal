CREATE TABLE `tb_izihmqlvrc` (
  `col_niyefsuvyu` time NOT NULL,
  `col_wtkibsyxum` blob(2345255919),
  `col_cdltiwnbvu` bigint,
  `col_prrfzwipjb` datetime(0) NULL,
  CONSTRAINT symb_fjxbkxuxip PRIMARY KEY (`col_niyefsuvyu`),
  UNIQUE KEY `uk_gdipvvuekw` (`col_cdltiwnbvu`),
  UNIQUE INDEX `uk_ksyxhyhtqd` (`col_wtkibsyxum`(22),`col_cdltiwnbvu`)
) DEFAULT CHARSET=utf8;
CREATE TABLE `tb_fnxaykfskl` (
  `col_rqdbfezzoe` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  UNIQUE `uk_lvhtqnyvmw` (`col_rqdbfezzoe`),
  UNIQUE INDEX `col_rqdbfezzoe` (`col_rqdbfezzoe`)
) DEFAULT CHARSET=latin1;
CREATE TABLE `tb_xyumhtxajs` (
  `col_vfmmubhple` datetime NULL,
  `col_dedkqeyizw` int unsigned NULL DEFAULT '1',
  `col_imfsauznol` longtext CHARACTER SET utf8mb4,
  `col_jqxasrchdv` double(240,6) NOT NULL,
  PRIMARY KEY (`col_jqxasrchdv`)
) DEFAULT CHARSET=utf8;
RENAME TABLE `tb_xyumhtxajs` TO `tb_dwdawlyulx`, `tb_izihmqlvrc` TO `tb_tqqjgqvlty`;
RENAME TABLE `tb_tqqjgqvlty` TO `tb_vltiykvgrd`;
RENAME TABLE `tb_vltiykvgrd` TO `tb_fswsddbzby`;
DROP TABLE tb_dwdawlyulx;
DROP TABLE tb_fnxaykfskl, tb_fswsddbzby;
CREATE TABLE `tb_jhqwojqbtj` (
  `col_byfkvdrjfx` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8mb4,
  `col_unzkrhvwzx` integer unsigned zerofill,
  `col_nnvyxvtluu` int(255) unsigned DEFAULT '1',
  `col_flcxltquqr` longtext CHARACTER SET latin1
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
ALTER TABLE `tb_jhqwojqbtj` ADD COLUMN (`col_tbaacjfxyz` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') NOT NULL, `col_xswcrnwfit` mediumblob);
ALTER TABLE `tb_jhqwojqbtj` ADD COLUMN `col_ksprsgkzrb` text(912789829) CHARACTER SET utf8mb4;
ALTER TABLE `tb_jhqwojqbtj` ADD COLUMN (`col_yhizzxtwet` integer unsigned, `col_mcyfebamvx` smallint(85) DEFAULT '1');
ALTER TABLE `tb_jhqwojqbtj` ADD (`col_bsjtvoujom` bit(55) NOT NULL DEFAULT b'0', `col_ndohgxbgox` float DEFAULT '1');
ALTER TABLE `tb_jhqwojqbtj` ADD (`col_egszzxhsvf` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') NOT NULL DEFAULT 'enum_or_set_0', `col_kycviiihvh` date);
ALTER TABLE `tb_jhqwojqbtj` ADD (`col_bqcpudnbtu` integer(189) NULL DEFAULT '1', `col_sexypgjnak` date DEFAULT '2019-07-04');
ALTER TABLE `tb_jhqwojqbtj` ADD `col_hemnlexcdd` int zerofill FIRST;
ALTER TABLE `tb_jhqwojqbtj` ADD (`col_pntvqnccno` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8mb4 NOT NULL, `col_vedygstsxf` timestamp(0) NULL DEFAULT CURRENT_TIMESTAMP(0));
ALTER TABLE `tb_jhqwojqbtj` ADD COLUMN (`col_qabskemxzo` tinyint(23) unsigned zerofill, `col_tnkspeksfi` tinytext);
ALTER TABLE `tb_jhqwojqbtj` ADD PRIMARY KEY (`col_tbaacjfxyz`);
ALTER TABLE `tb_jhqwojqbtj` ALTER `col_egszzxhsvf` DROP DEFAULT;
ALTER TABLE `tb_jhqwojqbtj` CHANGE `col_unzkrhvwzx` `col_gwajcvodth` year(4) NOT NULL DEFAULT '2019';
ALTER TABLE `tb_jhqwojqbtj` CHANGE COLUMN `col_bqcpudnbtu` `col_cuzojqplje` year DEFAULT '2019';
ALTER TABLE `tb_jhqwojqbtj` DROP `col_qabskemxzo`, DROP `col_vedygstsxf`;
ALTER TABLE `tb_jhqwojqbtj` DROP `col_byfkvdrjfx`;
ALTER TABLE `tb_jhqwojqbtj` DROP `col_tbaacjfxyz`, DROP `col_kycviiihvh`;
ALTER TABLE `tb_jhqwojqbtj` DROP `col_mcyfebamvx`;
ALTER TABLE `tb_jhqwojqbtj` DROP COLUMN `col_yhizzxtwet`, DROP COLUMN `col_bsjtvoujom`;
ALTER TABLE `tb_jhqwojqbtj` DROP `col_tnkspeksfi`, DROP `col_xswcrnwfit`;
ALTER TABLE `tb_jhqwojqbtj` DROP `col_nnvyxvtluu`;
ALTER TABLE `tb_jhqwojqbtj` DROP `col_pntvqnccno`, DROP `col_hemnlexcdd`;
ALTER TABLE `tb_jhqwojqbtj` DROP `col_gwajcvodth`;
