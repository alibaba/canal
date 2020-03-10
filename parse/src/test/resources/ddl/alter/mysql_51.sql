CREATE TABLE `tb_krsikytrbw` (
  `col_lliihnoyoo` int(145) NOT NULL DEFAULT '1',
  `col_qkgtxzzfie` tinyblob,
  `col_lfbganjgad` timestamp(4) NULL DEFAULT NULL,
  `col_cqjptjcgyb` binary(1) NOT NULL,
  `col_lfptlilmsv` int(230) unsigned zerofill NOT NULL,
  `col_omxbpiulnd` year(4) DEFAULT '2019',
  PRIMARY KEY (`col_cqjptjcgyb`),
  UNIQUE KEY `col_cqjptjcgyb` (`col_cqjptjcgyb`,`col_lliihnoyoo`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
