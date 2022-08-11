CREATE TABLE `tb_zbnbjecfad` (
  `col_hiezvpgyuu` timestamp(4) NOT NULL DEFAULT CURRENT_TIMESTAMP(4),
  `col_vydoxpfwit` float(202,3) NULL,
  `col_kdvvwclils` year(4) DEFAULT '2019',
  `col_ovrngczyyk` mediumint(81) unsigned,
  UNIQUE `col_hiezvpgyuu` (`col_hiezvpgyuu`,`col_vydoxpfwit`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE TABLE `tb_nkikxbvhfw` (
  `col_swzsjdffqv` blob(2134010468),
  UNIQUE INDEX `col_swzsjdffqv` (`col_swzsjdffqv`(24)),
  UNIQUE `col_swzsjdffqv_2` (`col_swzsjdffqv`(32))
) DEFAULT CHARSET=utf8;
RENAME TABLE `tb_nkikxbvhfw` TO `tb_cihgbeacba`, `tb_zbnbjecfad` TO `tb_mlsrpqwnhf`;
DROP TABLE tb_cihgbeacba;
ALTER TABLE `tb_mlsrpqwnhf` ADD COLUMN `col_mfdevzanga` tinyint zerofill NOT NULL;
ALTER TABLE `tb_mlsrpqwnhf` ADD COLUMN `col_yualkpafem` double(197,7);
ALTER TABLE `tb_mlsrpqwnhf` ADD (`col_cmghpeldvl` mediumtext CHARACTER SET utf8, `col_ooogllngga` decimal(38,8) NULL);
ALTER TABLE `tb_mlsrpqwnhf` ADD `col_fkklsgzldp` numeric NOT NULL;
ALTER TABLE `tb_mlsrpqwnhf` ADD COLUMN (`col_bdqubzeijx` tinytext, `col_yeligvvmhm` tinytext CHARACTER SET utf8mb4);
ALTER TABLE `tb_mlsrpqwnhf` CHARACTER SET utf8;
ALTER TABLE `tb_mlsrpqwnhf` ADD CONSTRAINT PRIMARY KEY (`col_hiezvpgyuu`);
ALTER TABLE `tb_mlsrpqwnhf` ADD UNIQUE INDEX `uk_jybutmebnj` (`col_fkklsgzldp`,`col_yeligvvmhm`(2));
ALTER TABLE `tb_mlsrpqwnhf` ALTER `col_ovrngczyyk` DROP DEFAULT;
ALTER TABLE `tb_mlsrpqwnhf` ALTER COLUMN `col_mfdevzanga` DROP DEFAULT;
ALTER TABLE `tb_mlsrpqwnhf` CHANGE COLUMN `col_vydoxpfwit` `col_mitqygycnq` datetime(3) NULL FIRST;
ALTER TABLE `tb_mlsrpqwnhf` DROP COLUMN `col_hiezvpgyuu`, DROP COLUMN `col_ovrngczyyk`;
ALTER TABLE `tb_mlsrpqwnhf` DROP `col_fkklsgzldp`, DROP `col_ooogllngga`;
ALTER TABLE `tb_mlsrpqwnhf` DROP COLUMN `col_mfdevzanga`, DROP COLUMN `col_yualkpafem`;
ALTER TABLE `tb_mlsrpqwnhf` DROP `col_mitqygycnq`, DROP `col_yeligvvmhm`;
ALTER TABLE `tb_mlsrpqwnhf` DROP `col_cmghpeldvl`, DROP `col_bdqubzeijx`;
