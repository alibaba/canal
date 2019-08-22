CREATE TABLE `tb_srnygbtlet` (
  `col_lifsfguaso` bigint unsigned zerofill NOT NULL,
  `col_jfqoynkdul` int(85) zerofill NOT NULL,
  `col_xtdeqhkius` int unsigned zerofill
) DEFAULT CHARSET=utf8;
CREATE TABLE `tb_sykueansru` (
  `col_tvoyslqfaz` char(165) CHARACTER SET utf8 NOT NULL,
  `col_kcqzudwksh` int(74) zerofill,
  CONSTRAINT PRIMARY KEY (`col_tvoyslqfaz`(15)),
  UNIQUE `uk_jusgrekcyt` (`col_tvoyslqfaz`(10))
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `tb_ljqwtifhmg` LIKE `tb_srnygbtlet`;
RENAME TABLE `tb_srnygbtlet` TO `tb_cvldhcmtfi`;
RENAME TABLE `tb_sykueansru` TO `tb_yizqtrqbqn`;
RENAME TABLE `tb_cvldhcmtfi` TO `tb_vloxpqyomb`;
DROP TABLE tb_yizqtrqbqn, tb_vloxpqyomb;
ALTER TABLE `tb_ljqwtifhmg` ADD (`col_jskeqnympm` mediumblob, `col_reyjvzjrca` mediumtext);
ALTER TABLE `tb_ljqwtifhmg` ADD `col_vqkcruulcb` numeric NOT NULL DEFAULT '1' AFTER `col_xtdeqhkius`;
ALTER TABLE `tb_ljqwtifhmg` ADD `col_udtcfawuge` year NOT NULL;
ALTER TABLE `tb_ljqwtifhmg` ADD COLUMN `col_zkypjejoqz` longtext CHARACTER SET utf8;
ALTER TABLE `tb_ljqwtifhmg` ADD COLUMN `col_ebgvlcdynr` tinyint(233) unsigned DEFAULT '1' AFTER `col_jskeqnympm`;
ALTER TABLE `tb_ljqwtifhmg` ADD COLUMN `col_hfnljoelph` blob;
ALTER TABLE `tb_ljqwtifhmg` ADD `col_vtvtsqhwby` decimal DEFAULT '1';
ALTER TABLE `tb_ljqwtifhmg` CHARACTER SET = utf8;
ALTER TABLE `tb_ljqwtifhmg` ADD UNIQUE `uk_geldgbtngh` (`col_reyjvzjrca`(10),`col_udtcfawuge`);
ALTER TABLE `tb_ljqwtifhmg` ADD UNIQUE (`col_vqkcruulcb`,`col_jskeqnympm`(32));
ALTER TABLE `tb_ljqwtifhmg` CHANGE `col_jfqoynkdul` `col_vlntyfqpdc` longtext CHARACTER SET utf8mb4;
ALTER TABLE `tb_ljqwtifhmg` DROP COLUMN `col_ebgvlcdynr`, DROP COLUMN `col_vqkcruulcb`;
ALTER TABLE `tb_ljqwtifhmg` DROP `col_hfnljoelph`, DROP `col_vtvtsqhwby`;
ALTER TABLE `tb_ljqwtifhmg` DROP COLUMN `col_xtdeqhkius`;
ALTER TABLE `tb_ljqwtifhmg` DROP `col_vlntyfqpdc`, DROP `col_zkypjejoqz`;
ALTER TABLE `tb_ljqwtifhmg` DROP COLUMN `col_udtcfawuge`;
ALTER TABLE `tb_ljqwtifhmg` DROP `col_reyjvzjrca`;
ALTER TABLE `tb_ljqwtifhmg` DROP COLUMN `col_jskeqnympm`;
