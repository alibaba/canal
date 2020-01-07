CREATE TABLE `tb_ucmkmerovj` (
  `col_hcfhnqdpda` tinyblob
) DEFAULT CHARSET=utf8;
CREATE TABLE `tb_nsjknrezjn` LIKE `tb_ucmkmerovj`;
RENAME TABLE `tb_ucmkmerovj` TO `tb_spnawqlyqo`, `tb_nsjknrezjn` TO `tb_ezrzlemqcq`;
RENAME TABLE `tb_spnawqlyqo` TO `tb_wbbrvurmym`;
DROP TABLE tb_wbbrvurmym, tb_ezrzlemqcq;
CREATE TABLE `tb_odwnejztgw` (
  `col_grrazuxsmj` tinyint(108) unsigned zerofill,
  UNIQUE `col_grrazuxsmj` (`col_grrazuxsmj`),
  UNIQUE `uk_faxbizfzvg` (`col_grrazuxsmj`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
ALTER TABLE `tb_odwnejztgw` ADD COLUMN `col_dqxacqwdav` year NULL;
ALTER TABLE `tb_odwnejztgw` ADD COLUMN `col_afisennniu` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') NOT NULL DEFAULT 'enum_or_set_0';
ALTER TABLE `tb_odwnejztgw` ADD COLUMN `col_ikgmyfugbh` time(3) FIRST;
ALTER TABLE `tb_odwnejztgw` ADD COLUMN (`col_hsbiyreflo` mediumblob, `col_pmcsqydybm` bit(59));
ALTER TABLE `tb_odwnejztgw` ADD (`col_caewwurnsu` mediumblob, `col_oyxhvytofb` tinyint(62) unsigned zerofill);
ALTER TABLE `tb_odwnejztgw` ADD `col_hxgvemyiam` tinytext CHARACTER SET utf8;
ALTER TABLE `tb_odwnejztgw` ADD COLUMN `col_ouwtrbmtli` varbinary(39) NULL DEFAULT '\0' FIRST;
ALTER TABLE `tb_odwnejztgw` DEFAULT CHARACTER SET = utf8;
ALTER TABLE `tb_odwnejztgw` ADD CONSTRAINT symb_pztcjvefkb PRIMARY KEY (`col_afisennniu`);
ALTER TABLE `tb_odwnejztgw` CHANGE COLUMN `col_oyxhvytofb` `col_vumeeppqkm` int(62) zerofill;
ALTER TABLE `tb_odwnejztgw` DROP KEY `col_grrazuxsmj`;
ALTER TABLE `tb_odwnejztgw` DROP KEY `uk_faxbizfzvg`;
