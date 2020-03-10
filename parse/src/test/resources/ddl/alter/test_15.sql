CREATE TABLE `tb_odalwcctzo` (
  `col_zfwcescosm` bit,
  `col_gvrsiekgpu` mediumint NULL,
  `col_luknerkzbg` year
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `tb_ypebrnimav` LIKE `tb_odalwcctzo`;
RENAME TABLE `tb_odalwcctzo` TO `tb_hgbecqwvnf`, `tb_ypebrnimav` TO `tb_dqeiczpcpp`;
ALTER TABLE `tb_hgbecqwvnf` ADD COLUMN (`col_iomejxwfzh` timestamp, `col_dqpoxmoyzl` bigint(111) unsigned NULL DEFAULT '1');
ALTER TABLE `tb_hgbecqwvnf` ADD COLUMN (`col_ocmmfdzihr` tinytext, `col_thbfgmggyp` blob);
ALTER TABLE `tb_hgbecqwvnf` ADD COLUMN `col_nyrvlflfiz` mediumblob FIRST;
ALTER TABLE `tb_hgbecqwvnf` ADD COLUMN `col_cdafuglmqd` mediumblob AFTER `col_ocmmfdzihr`;
ALTER TABLE `tb_hgbecqwvnf` ADD (`col_ibpfuqwtka` integer(152) zerofill NOT NULL, `col_mfbftkexwt` time(4) NULL);
ALTER TABLE `tb_hgbecqwvnf` ADD `col_rfnetihhhs` numeric(15) NOT NULL AFTER `col_ibpfuqwtka`;
ALTER TABLE `tb_hgbecqwvnf` ADD `col_shfhtlxxhg` bigint unsigned zerofill;
ALTER TABLE `tb_hgbecqwvnf` ADD (`col_jhaqzczhxe` mediumblob, `col_gfdmpyoigq` time(1));
ALTER TABLE `tb_hgbecqwvnf` CHARACTER SET = utf8mb4;
ALTER TABLE `tb_hgbecqwvnf` ADD CONSTRAINT symb_vxlhuityyt PRIMARY KEY (`col_iomejxwfzh`);
ALTER TABLE `tb_hgbecqwvnf` ADD UNIQUE `uk_lsuitvpjev` (`col_iomejxwfzh`,`col_dqpoxmoyzl`);
ALTER TABLE `tb_hgbecqwvnf` ADD UNIQUE `col_luknerkzbg`(`col_luknerkzbg`,`col_dqpoxmoyzl`);
ALTER TABLE `tb_hgbecqwvnf` ALTER `col_rfnetihhhs` DROP DEFAULT;
ALTER TABLE `tb_hgbecqwvnf` ALTER COLUMN `col_dqpoxmoyzl` SET DEFAULT '1';
ALTER TABLE `tb_hgbecqwvnf` CHANGE `col_luknerkzbg` `col_ulsyzrcnlh` float(35) NULL;
ALTER TABLE `tb_hgbecqwvnf` CHANGE `col_ulsyzrcnlh` `col_qppnejcxiu` binary(137) FIRST;
ALTER TABLE `tb_hgbecqwvnf` DROP COLUMN `col_shfhtlxxhg`;
ALTER TABLE `tb_hgbecqwvnf` DROP COLUMN `col_iomejxwfzh`, DROP COLUMN `col_cdafuglmqd`;
ALTER TABLE `tb_hgbecqwvnf` DROP COLUMN `col_thbfgmggyp`, DROP COLUMN `col_mfbftkexwt`;
ALTER TABLE `tb_hgbecqwvnf` DROP COLUMN `col_ibpfuqwtka`, DROP COLUMN `col_ocmmfdzihr`;
ALTER TABLE `tb_hgbecqwvnf` DROP COLUMN `col_gvrsiekgpu`;
ALTER TABLE `tb_hgbecqwvnf` DROP KEY `uk_lsuitvpjev`;
ALTER TABLE `tb_hgbecqwvnf` DROP INDEX `col_luknerkzbg`;
