CREATE TABLE `tb_imdtsfqnma` (
  `col_mbzhjizhhg` integer(14) unsigned,
  `col_lwcvaletgl` numeric(54,3),
  `col_vzmkkrtqea` bit(28) DEFAULT b'0',
  `col_rmtdktwjkf` time(3) NULL,
  UNIQUE KEY `uk_ejklfpuzdw` (`col_rmtdktwjkf`)
) DEFAULT CHARSET=utf8;
RENAME TABLE `tb_imdtsfqnma` TO `tb_hbqbstsanm`;
RENAME TABLE `tb_hbqbstsanm` TO `tb_uoquelzmdk`;
ALTER TABLE `tb_uoquelzmdk` ADD COLUMN (`col_bbvxosyuai` float, `col_wcdxzaykzl` smallint unsigned NULL DEFAULT '1');
ALTER TABLE `tb_uoquelzmdk` ADD `col_njhhehxbdq` mediumtext;
ALTER TABLE `tb_uoquelzmdk` ADD `col_znccfjlzyn` decimal(23,10);
ALTER TABLE `tb_uoquelzmdk` ADD (`col_gpuwdouomx` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8 NOT NULL, `col_wwdicmbkug` longblob);
ALTER TABLE `tb_uoquelzmdk` ADD (`col_bdbfyfsjoy` int(184) unsigned zerofill NULL, `col_eroukngokg` bigint(47) unsigned zerofill NOT NULL);
ALTER TABLE `tb_uoquelzmdk` ADD COLUMN (`col_mzeetglota` float(9) NOT NULL, `col_pheamargcl` smallint(239) zerofill);
ALTER TABLE `tb_uoquelzmdk` ADD `col_btyauewuzi` mediumblob AFTER `col_bbvxosyuai`;
ALTER TABLE `tb_uoquelzmdk` ADD UNIQUE KEY `uk_sjpsswkher` (`col_bdbfyfsjoy`,`col_pheamargcl`);
ALTER TABLE `tb_uoquelzmdk` ALTER `col_lwcvaletgl` DROP DEFAULT;
ALTER TABLE `tb_uoquelzmdk` ALTER COLUMN `col_pheamargcl` DROP DEFAULT;
ALTER TABLE `tb_uoquelzmdk` DROP COLUMN `col_bbvxosyuai`;
ALTER TABLE `tb_uoquelzmdk` DROP `col_lwcvaletgl`;
ALTER TABLE `tb_uoquelzmdk` DROP COLUMN `col_eroukngokg`, DROP COLUMN `col_mzeetglota`;
ALTER TABLE `tb_uoquelzmdk` DROP COLUMN `col_znccfjlzyn`, DROP COLUMN `col_vzmkkrtqea`;
ALTER TABLE `tb_uoquelzmdk` DROP COLUMN `col_rmtdktwjkf`;
ALTER TABLE `tb_uoquelzmdk` DROP COLUMN `col_gpuwdouomx`;
ALTER TABLE `tb_uoquelzmdk` DROP `col_pheamargcl`, DROP `col_btyauewuzi`;
ALTER TABLE `tb_uoquelzmdk` DROP COLUMN `col_mbzhjizhhg`, DROP COLUMN `col_bdbfyfsjoy`;
