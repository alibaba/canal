CREATE TABLE `tb_qhsljnukja` (
  `col_fersnxhqir` numeric(42) NULL,
  `col_lzdbrgiewo` tinyblob,
  `col_xdshuwzznl` binary(156) NOT NULL,
  `col_wguqpgfoll` double NOT NULL DEFAULT '1',
  CONSTRAINT PRIMARY KEY (`col_xdshuwzznl`(19),`col_wguqpgfoll`)
) DEFAULT CHARSET=latin1;
CREATE TABLE `tb_drxwrxamak` (
  `col_ijririnvpk` varbinary(57),
  `col_pzzdnkysqx` int unsigned zerofill NOT NULL,
  `col_nsnfutrvvi` double(20,8)
) DEFAULT CHARSET=utf8;
CREATE TABLE `tb_rexvlcurcc` LIKE `tb_drxwrxamak`;
CREATE TABLE `tb_exsgxyoern` (
  `col_cnvlgnafnx` date,
  `col_chjcwavubn` int(73) unsigned,
  `col_jihokqqpqw` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8 DEFAULT 'enum_or_set_0'
) DEFAULT CHARSET=utf8;
CREATE TABLE `tb_kjyszidrak` (
  `col_cbvwqvuuvo` varbinary(128) NOT NULL,
  `col_kvspyjtqjg` text CHARACTER SET utf8,
  CONSTRAINT UNIQUE `uk_rjyzqluaky` (`col_cbvwqvuuvo`(3))
) DEFAULT CHARSET=utf8;
RENAME TABLE `tb_exsgxyoern` TO `tb_jjevlzpizx`;
RENAME TABLE `tb_jjevlzpizx` TO `tb_bckwmwbhxe`;
RENAME TABLE `tb_rexvlcurcc` TO `tb_reoiwwcvxn`, `tb_drxwrxamak` TO `tb_yewsyhywwt`;
RENAME TABLE `tb_kjyszidrak` TO `tb_jaaspokopu`, `tb_reoiwwcvxn` TO `tb_zcarrcfbqk`;
RENAME TABLE `tb_yewsyhywwt` TO `tb_qfbjzrlhat`, `tb_qhsljnukja` TO `tb_olxipktwgn`;
DROP TABLE tb_zcarrcfbqk;
DROP TABLE tb_bckwmwbhxe;
DROP TABLE tb_olxipktwgn;
DROP TABLE tb_qfbjzrlhat;
