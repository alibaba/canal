CREATE TABLE `tb_rvyjlenqnq` (
  `col_bvvlapfogy` tinyint zerofill,
  `col_bkqxmqavul` decimal(63,16) NOT NULL,
  `col_apojimldou` timestamp NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
CREATE TABLE `tb_epfxexadow` (
  `col_fuowyuxzjz` tinyint(117) unsigned DEFAULT '1',
  `col_tkevazcpbc` numeric(36,23)
) DEFAULT CHARSET=utf8;
RENAME TABLE `tb_rvyjlenqnq` TO `tb_keyyesvarm`, `tb_epfxexadow` TO `tb_pbwzafanju`;
DROP TABLE tb_pbwzafanju;
ALTER TABLE `tb_keyyesvarm` ADD `col_liorapkbnd` mediumint(65) zerofill NOT NULL;
ALTER TABLE `tb_keyyesvarm` ADD `col_zixordgmdc` decimal(3,1);
ALTER TABLE `tb_keyyesvarm` ADD COLUMN `col_muylgrtcdh` varchar(79) CHARACTER SET utf8mb4;
ALTER TABLE `tb_keyyesvarm` ADD `col_xxwqoepjpj` char AFTER `col_bvvlapfogy`;
ALTER TABLE `tb_keyyesvarm` ADD COLUMN (`col_qttxalrego` blob(3354084938), `col_ycfogbjylp` binary(106));
ALTER TABLE `tb_keyyesvarm` ADD COLUMN (`col_anvpdwbfbo` smallint zerofill, `col_victdeyala` decimal(48,7));
ALTER TABLE `tb_keyyesvarm` ADD (`col_ftspfhlmkw` tinyblob, `col_uvxqqmoipg` char CHARACTER SET utf8);
ALTER TABLE `tb_keyyesvarm` ADD (`col_zfowehmbto` bit NULL DEFAULT b'0', `col_lqkravajqi` longtext);
ALTER TABLE `tb_keyyesvarm` ADD COLUMN `col_kyzsjpzdyk` numeric(41);
ALTER TABLE `tb_keyyesvarm` ADD UNIQUE INDEX (`col_apojimldou`,`col_liorapkbnd`);
ALTER TABLE `tb_keyyesvarm` ALTER COLUMN `col_uvxqqmoipg` DROP DEFAULT;
ALTER TABLE `tb_keyyesvarm` ALTER `col_victdeyala` SET DEFAULT NULL;
ALTER TABLE `tb_keyyesvarm` DROP `col_uvxqqmoipg`;
ALTER TABLE `tb_keyyesvarm` DROP COLUMN `col_muylgrtcdh`, DROP COLUMN `col_anvpdwbfbo`;
ALTER TABLE `tb_keyyesvarm` DROP COLUMN `col_bkqxmqavul`;
ALTER TABLE `tb_keyyesvarm` DROP COLUMN `col_ycfogbjylp`, DROP COLUMN `col_zfowehmbto`;
ALTER TABLE `tb_keyyesvarm` DROP `col_xxwqoepjpj`, DROP `col_apojimldou`;
ALTER TABLE `tb_keyyesvarm` DROP `col_liorapkbnd`;
ALTER TABLE `tb_keyyesvarm` DROP `col_ftspfhlmkw`, DROP `col_kyzsjpzdyk`;
ALTER TABLE `tb_keyyesvarm` DROP COLUMN `col_bvvlapfogy`, DROP COLUMN `col_qttxalrego`;
