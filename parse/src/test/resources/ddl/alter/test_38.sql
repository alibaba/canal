CREATE TABLE `tb_axmikqwoki` (
  `col_emfohlohwo` bit NULL,
  `col_ubwarukvbl` tinyint(141) unsigned zerofill NOT NULL,
  `col_amwzbhvmje` numeric NOT NULL DEFAULT '1',
  `col_qrklyrbbgp` time(6) NOT NULL,
  UNIQUE `uk_bbaanqoyjv` (`col_qrklyrbbgp`),
  UNIQUE KEY `col_amwzbhvmje` (`col_amwzbhvmje`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `tb_qiuxoamkjv` (
  `col_sahfvmjolk` tinyblob,
  `col_facdiejxet` mediumtext CHARACTER SET utf8,
  UNIQUE `col_facdiejxet` (`col_facdiejxet`(6))
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `tb_jtxcznanun` (
  `col_qkjsktvtdg` decimal(9,7) NOT NULL,
  UNIQUE `uk_qwtaypasqp` (`col_qkjsktvtdg`),
  UNIQUE INDEX `col_qkjsktvtdg` (`col_qkjsktvtdg`)
) DEFAULT CHARSET=utf8;
RENAME TABLE `tb_jtxcznanun` TO `tb_djmwhfbubf`, `tb_axmikqwoki` TO `tb_unuzjkojlt`;
DROP TABLE tb_unuzjkojlt, tb_djmwhfbubf;
DROP TABLE tb_qiuxoamkjv;
CREATE TABLE `tb_oxwwculmvu` (
  `col_msksigujbh` text CHARACTER SET utf8,
  `col_qfmpogxwgh` mediumtext CHARACTER SET utf8,
  UNIQUE `uk_qfabuzevew` (`col_msksigujbh`(16))
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
ALTER TABLE `tb_oxwwculmvu` ADD COLUMN (`col_kesjpzxctq` datetime(6), `col_szncpsegwq` time NULL DEFAULT '00:00:00');
ALTER TABLE `tb_oxwwculmvu` ADD (`col_wxqxqueihr` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') NULL, `col_hyhtklzrvn` varchar(147) CHARACTER SET utf8 NOT NULL DEFAULT '');
ALTER TABLE `tb_oxwwculmvu` ADD (`col_zrjuysqdco` smallint(247) unsigned, `col_umxbmdtpxs` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') NOT NULL DEFAULT 'enum_or_set_0');
ALTER TABLE `tb_oxwwculmvu` ADD COLUMN `col_bewssrhuze` tinytext CHARACTER SET utf8 COLLATE utf8_unicode_ci;
ALTER TABLE `tb_oxwwculmvu` ADD `col_jnwlatgrgv` mediumblob FIRST;
ALTER TABLE `tb_oxwwculmvu` ADD COLUMN `col_xemgprlpmx` double(138,3) FIRST;
ALTER TABLE `tb_oxwwculmvu` ADD COLUMN (`col_yyixefgbqi` year(4), `col_kixpfwoyyq` tinytext CHARACTER SET utf8);
ALTER TABLE `tb_oxwwculmvu` ADD `col_wsgiswplbj` decimal(7,4);
ALTER TABLE `tb_oxwwculmvu` ADD UNIQUE `uk_vrydxlndik` (`col_zrjuysqdco`,`col_yyixefgbqi`);
ALTER TABLE `tb_oxwwculmvu` ADD UNIQUE (`col_wsgiswplbj`);
ALTER TABLE `tb_oxwwculmvu` ALTER COLUMN `col_kesjpzxctq` DROP DEFAULT;
ALTER TABLE `tb_oxwwculmvu` ALTER `col_umxbmdtpxs` DROP DEFAULT;
ALTER TABLE `tb_oxwwculmvu` ALTER COLUMN `col_wsgiswplbj` DROP DEFAULT;
ALTER TABLE `tb_oxwwculmvu` CHANGE COLUMN `col_wsgiswplbj` `col_qxmxrwuujo` integer(209) unsigned zerofill NOT NULL FIRST;
ALTER TABLE `tb_oxwwculmvu` CHANGE `col_kesjpzxctq` `col_gspziyqoeg` blob;
ALTER TABLE `tb_oxwwculmvu` DROP COLUMN `col_xemgprlpmx`;
ALTER TABLE `tb_oxwwculmvu` DROP COLUMN `col_jnwlatgrgv`, DROP COLUMN `col_gspziyqoeg`;
ALTER TABLE `tb_oxwwculmvu` DROP COLUMN `col_zrjuysqdco`, DROP COLUMN `col_msksigujbh`;
ALTER TABLE `tb_oxwwculmvu` DROP COLUMN `col_qxmxrwuujo`;
ALTER TABLE `tb_oxwwculmvu` DROP COLUMN `col_qfmpogxwgh`, DROP COLUMN `col_kixpfwoyyq`;
