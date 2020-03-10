CREATE TABLE `tb_qinfehbzem` (
  `col_ginzfhyvfi` int unsigned NULL DEFAULT '1',
  `col_cakcnwuchg` date DEFAULT '2019-07-04',
  `col_hgdcocydeg` integer unsigned zerofill,
  `col_opsrfpouaa` longblob,
  UNIQUE `uk_tcqbsivphi` (`col_opsrfpouaa`(1)),
  UNIQUE INDEX `col_hgdcocydeg` (`col_hgdcocydeg`,`col_opsrfpouaa`(29))
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE TABLE `tb_gcwttmsayc` (
  `col_rwfupswrnp` tinytext CHARACTER SET utf8mb4,
  UNIQUE KEY `uk_rqatmzledw` (`col_rwfupswrnp`(6))
) DEFAULT CHARSET=latin1;
CREATE TABLE `tb_wvnpljeqpj` (
  `col_qrgsrjkyom` double(87,17) NULL,
  UNIQUE INDEX `col_qrgsrjkyom` (`col_qrgsrjkyom`),
  UNIQUE INDEX `col_qrgsrjkyom_2` (`col_qrgsrjkyom`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE TABLE `tb_kezjbtsebu` LIKE `tb_gcwttmsayc`;
CREATE TABLE `tb_cjwgkhdfpa` LIKE `tb_gcwttmsayc`;
CREATE TABLE `tb_uvqnvkckwl` LIKE `tb_gcwttmsayc`;
CREATE TABLE `tb_rcqcegxwfy` (
  `col_crzrgcsrtd` int NOT NULL,
  `col_hkaqkrqzfj` mediumtext CHARACTER SET utf8mb4,
  `col_wobtyvzfom` blob,
  CONSTRAINT `symb_lyzhvxawge` UNIQUE INDEX `uk_gomrdpfzlr` (`col_crzrgcsrtd`),
  UNIQUE `uk_xcqwpumviu` (`col_crzrgcsrtd`,`col_wobtyvzfom`(4))
) DEFAULT CHARSET=utf8;
RENAME TABLE `tb_gcwttmsayc` TO `tb_kwqumakbsd`;
RENAME TABLE `tb_wvnpljeqpj` TO `tb_moiidbfdwn`, `tb_uvqnvkckwl` TO `tb_avkqzktrix`;
RENAME TABLE `tb_rcqcegxwfy` TO `tb_rtbwxgyymu`, `tb_kezjbtsebu` TO `tb_qddvuiqxts`;
RENAME TABLE `tb_qddvuiqxts` TO `tb_dxqnwmkzok`;
RENAME TABLE `tb_moiidbfdwn` TO `tb_zhkwttzekg`;
DROP TABLE tb_zhkwttzekg, tb_cjwgkhdfpa;
DROP TABLE tb_rtbwxgyymu, tb_dxqnwmkzok;
DROP TABLE tb_kwqumakbsd;
DROP TABLE tb_avkqzktrix;
