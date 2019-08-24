CREATE TABLE `tb_cvctdzexnw` (
  `col_ubxpjfudbv` timestamp(5) DEFAULT CURRENT_TIMESTAMP(5),
  `col_otmxxxfiqk` year(4) NOT NULL,
  `col_ctnmteeugh` tinytext CHARACTER SET utf8mb4,
  UNIQUE KEY `col_ubxpjfudbv` (`col_ubxpjfudbv`,`col_otmxxxfiqk`)
) DEFAULT CHARSET=latin1;
RENAME TABLE `tb_cvctdzexnw` TO `tb_yzcwyrztvj`;
ALTER TABLE `tb_yzcwyrztvj` ADD `col_oowroifdzd` mediumblob;
ALTER TABLE `tb_yzcwyrztvj` ADD COLUMN (`col_vnqmuvuxjr` tinyblob, `col_jmsmixjrlt` binary(244) NULL);
ALTER TABLE `tb_yzcwyrztvj` ADD COLUMN (`col_hhnmmxmqtx` int(213) unsigned zerofill NULL, `col_koijxckugd` text(2232458377) CHARACTER SET utf8mb4);
ALTER TABLE `tb_yzcwyrztvj` ADD COLUMN (`col_bjzaoslady` float(19) NULL, `col_gyvlawjxpq` numeric NOT NULL);
ALTER TABLE `tb_yzcwyrztvj` ADD PRIMARY KEY (`col_ubxpjfudbv`);
ALTER TABLE `tb_yzcwyrztvj` ADD UNIQUE KEY `uk_swcaqrkugf` (`col_otmxxxfiqk`,`col_ctnmteeugh`(1));
ALTER TABLE `tb_yzcwyrztvj` ADD UNIQUE (`col_jmsmixjrlt`(22),`col_hhnmmxmqtx`);
ALTER TABLE `tb_yzcwyrztvj` ALTER `col_otmxxxfiqk` SET DEFAULT '2019';
ALTER TABLE `tb_yzcwyrztvj` CHANGE `col_oowroifdzd` `col_oqzqcdxhvk` decimal(35,30) AFTER `col_koijxckugd`;
ALTER TABLE `tb_yzcwyrztvj` DROP `col_gyvlawjxpq`, DROP `col_ctnmteeugh`;
ALTER TABLE `tb_yzcwyrztvj` DROP `col_oqzqcdxhvk`;
ALTER TABLE `tb_yzcwyrztvj` DROP COLUMN `col_koijxckugd`;
ALTER TABLE `tb_yzcwyrztvj` DROP COLUMN `col_otmxxxfiqk`, DROP COLUMN `col_vnqmuvuxjr`;
ALTER TABLE `tb_yzcwyrztvj` DROP COLUMN `col_jmsmixjrlt`;
ALTER TABLE `tb_yzcwyrztvj` DROP `col_hhnmmxmqtx`;
ALTER TABLE `tb_yzcwyrztvj` DROP COLUMN `col_bjzaoslady`;
ALTER TABLE `tb_yzcwyrztvj` DROP KEY `col_ubxpjfudbv`;
