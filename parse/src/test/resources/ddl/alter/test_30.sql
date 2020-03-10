CREATE TABLE `tb_tmxzhiadaj` (
  `col_gdnftbskks` datetime(4) NULL,
  `col_crzzjsyeyk` tinyblob,
  UNIQUE KEY `uk_splpaskbbf` (`col_gdnftbskks`),
  UNIQUE KEY `uk_gcdkboipfa` (`col_gdnftbskks`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE TABLE `tb_jmcsxwqpdc` (
  `col_kfjyiiexjd` timestamp,
  CONSTRAINT symb_cbacpsgnkl PRIMARY KEY (`col_kfjyiiexjd`)
) DEFAULT CHARSET=latin1;
CREATE TABLE `tb_pfzgcmldfg` LIKE `tb_tmxzhiadaj`;
RENAME TABLE `tb_tmxzhiadaj` TO `tb_ruvaoezmwl`;
RENAME TABLE `tb_jmcsxwqpdc` TO `tb_jchakrwydf`;
DROP TABLE tb_jchakrwydf;
ALTER TABLE `tb_pfzgcmldfg` ADD `col_zgrykqfrec` tinytext CHARACTER SET utf8 COLLATE utf8_unicode_ci;
ALTER TABLE `tb_pfzgcmldfg` ADD COLUMN `col_fpwqjmseog` numeric(12) NULL;
ALTER TABLE `tb_pfzgcmldfg` ADD (`col_sqfwaxyktx` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') NULL DEFAULT 'enum_or_set_0', `col_duzzugyump` smallint(74) unsigned zerofill NULL);
ALTER TABLE `tb_pfzgcmldfg` ADD (`col_gmbaiqhote` varbinary(140) NOT NULL, `col_nntmqziejp` text);
ALTER TABLE `tb_pfzgcmldfg` ADD (`col_hqjdekphma` date NULL, `col_lbikfetujc` timestamp(6) DEFAULT CURRENT_TIMESTAMP(6));
ALTER TABLE `tb_pfzgcmldfg` ADD (`col_enjwbyuifx` longblob, `col_btttknjcig` tinyblob);
ALTER TABLE `tb_pfzgcmldfg` ADD COLUMN (`col_rtstrcujvm` bigint unsigned NOT NULL, `col_qpjwuqjymd` varchar(209) NOT NULL DEFAULT '');
ALTER TABLE `tb_pfzgcmldfg` CHARACTER SET utf8;
ALTER TABLE `tb_pfzgcmldfg` ADD PRIMARY KEY (`col_gmbaiqhote`(13),`col_lbikfetujc`);
ALTER TABLE `tb_pfzgcmldfg` ADD UNIQUE KEY (`col_enjwbyuifx`(20),`col_btttknjcig`(1));
ALTER TABLE `tb_pfzgcmldfg` ADD UNIQUE `uk_tkfeqlqmie` (`col_sqfwaxyktx`,`col_gmbaiqhote`(2));
ALTER TABLE `tb_pfzgcmldfg` ALTER `col_qpjwuqjymd` DROP DEFAULT;
ALTER TABLE `tb_pfzgcmldfg` DROP `col_fpwqjmseog`, DROP `col_qpjwuqjymd`;
ALTER TABLE `tb_pfzgcmldfg` DROP `col_gmbaiqhote`, DROP `col_zgrykqfrec`;
ALTER TABLE `tb_pfzgcmldfg` DROP `col_rtstrcujvm`, DROP `col_crzzjsyeyk`;
ALTER TABLE `tb_pfzgcmldfg` DROP `col_sqfwaxyktx`;
ALTER TABLE `tb_pfzgcmldfg` DROP COLUMN `col_lbikfetujc`;
ALTER TABLE `tb_pfzgcmldfg` DROP `col_btttknjcig`;
ALTER TABLE `tb_pfzgcmldfg` DROP `col_enjwbyuifx`;
ALTER TABLE `tb_pfzgcmldfg` DROP `col_hqjdekphma`, DROP `col_nntmqziejp`;
ALTER TABLE `tb_pfzgcmldfg` DROP INDEX `uk_splpaskbbf`;
