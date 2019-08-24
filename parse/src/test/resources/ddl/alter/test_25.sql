CREATE TABLE `tb_kvzhyqqpam` (
  `col_lyptirnsfn` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8mb4 NOT NULL,
  `col_cqxknjvdgb` blob,
  `col_xavhqavxqo` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET latin1 NOT NULL DEFAULT 'enum_or_set_0',
  `col_lmxzibarzh` mediumblob,
  CONSTRAINT symb_kvlbvjhbln PRIMARY KEY (`col_lyptirnsfn`),
  UNIQUE KEY `col_lyptirnsfn` (`col_lyptirnsfn`,`col_xavhqavxqo`)
) DEFAULT CHARSET=latin1;
RENAME TABLE `tb_kvzhyqqpam` TO `tb_tspkdvcnhl`;
RENAME TABLE `tb_tspkdvcnhl` TO `tb_wqjpnnzouv`;
RENAME TABLE `tb_wqjpnnzouv` TO `tb_aafdasghga`;
ALTER TABLE `tb_aafdasghga` ADD (`col_uayhwfkoln` longtext CHARACTER SET utf8mb4, `col_qskwbkbegb` decimal(16));
ALTER TABLE `tb_aafdasghga` ADD COLUMN (`col_wnhexsbjnb` decimal(9) NULL, `col_fxicikwsct` float(24) NOT NULL);
ALTER TABLE `tb_aafdasghga` ADD COLUMN (`col_spzdmnayxy` char(190), `col_wmxfqcqztv` time NOT NULL DEFAULT '00:00:00');
ALTER TABLE `tb_aafdasghga` ADD COLUMN (`col_ygghjheerk` text CHARACTER SET utf8, `col_zbenqlvted` binary NULL);
ALTER TABLE `tb_aafdasghga` ADD `col_ixseptopfk` date DEFAULT '2019-07-04';
ALTER TABLE `tb_aafdasghga` ADD `col_pslvsvljbg` longblob;
ALTER TABLE `tb_aafdasghga` ADD COLUMN `col_ispobmxiik` year;
ALTER TABLE `tb_aafdasghga` ADD (`col_iafihazdod` char NOT NULL, `col_tfuomyukus` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') DEFAULT 'enum_or_set_0');
ALTER TABLE `tb_aafdasghga` ADD UNIQUE (`col_fxicikwsct`,`col_spzdmnayxy`(21));
ALTER TABLE `tb_aafdasghga` ADD UNIQUE (`col_zbenqlvted`,`col_pslvsvljbg`(10));
ALTER TABLE `tb_aafdasghga` CHANGE COLUMN `col_cqxknjvdgb` `col_rasjhueqqp` mediumint zerofill NOT NULL;
ALTER TABLE `tb_aafdasghga` DROP `col_lmxzibarzh`, DROP `col_rasjhueqqp`;
ALTER TABLE `tb_aafdasghga` DROP `col_wmxfqcqztv`;
ALTER TABLE `tb_aafdasghga` DROP `col_spzdmnayxy`, DROP `col_pslvsvljbg`;
ALTER TABLE `tb_aafdasghga` DROP `col_ispobmxiik`;
ALTER TABLE `tb_aafdasghga` DROP COLUMN `col_wnhexsbjnb`;
ALTER TABLE `tb_aafdasghga` DROP COLUMN `col_lyptirnsfn`;
ALTER TABLE `tb_aafdasghga` DROP COLUMN `col_tfuomyukus`;
ALTER TABLE `tb_aafdasghga` DROP COLUMN `col_xavhqavxqo`;
