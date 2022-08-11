CREATE TABLE `tb_fvjncpgwhy` (
  `col_twxddkipof` bit DEFAULT b'0',
  `col_gsqeppbqjg` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8mb4,
  `col_yeejeykwvz` mediumblob,
  `col_icjtmriski` varbinary(56) NULL,
  UNIQUE `uk_lqliirowvk` (`col_yeejeykwvz`(18))
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `tb_okrocudiku` (
  `col_ksrzieegxf` smallint(50) zerofill NULL,
  `col_resqomiafx` tinyblob
) DEFAULT CHARSET=utf8;
RENAME TABLE `tb_okrocudiku` TO `tb_bozkivuenk`, `tb_fvjncpgwhy` TO `tb_cegumineka`;
RENAME TABLE `tb_cegumineka` TO `tb_xmvawoekwx`;
DROP TABLE tb_bozkivuenk;
ALTER TABLE `tb_xmvawoekwx` ADD COLUMN `col_jzpxlsfskd` varbinary(49) DEFAULT '\0' FIRST;
ALTER TABLE `tb_xmvawoekwx` CHARACTER SET utf8mb4;
ALTER TABLE `tb_xmvawoekwx` ADD UNIQUE INDEX `uk_gkkpkxazqo` (`col_yeejeykwvz`(18),`col_icjtmriski`(2));
ALTER TABLE `tb_xmvawoekwx` ADD UNIQUE INDEX `uk_sgrofvqufp` (`col_yeejeykwvz`(32),`col_icjtmriski`(2));
ALTER TABLE `tb_xmvawoekwx` CHANGE COLUMN `col_twxddkipof` `col_vvbbdbzdxx` longblob AFTER `col_yeejeykwvz`;
ALTER TABLE `tb_xmvawoekwx` DROP `col_yeejeykwvz`;
ALTER TABLE `tb_xmvawoekwx` DROP `col_jzpxlsfskd`;
ALTER TABLE `tb_xmvawoekwx` DROP `col_vvbbdbzdxx`, DROP `col_icjtmriski`;
