CREATE TABLE `tb_upzbnfzylo` (
  `col_dppomywdgn` tinyblob,
  `col_hybjpvnihc` mediumblob,
  `col_nvqsfunmve` set('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8 DEFAULT 'enum_or_set_0',
  `col_rvwyvmeidf` enum('enum_or_set_0','enum_or_set_1','enum_or_set_2') CHARACTER SET utf8 NOT NULL DEFAULT 'enum_or_set_0',
  CONSTRAINT PRIMARY KEY (`col_rvwyvmeidf`)
) DEFAULT CHARSET=utf8;
CREATE TABLE `tb_tfimbaoajc` LIKE `tb_upzbnfzylo`;
CREATE TABLE `tb_hovvshypke` (
  `col_psutzipcit` integer(29) zerofill,
  UNIQUE INDEX `col_psutzipcit` (`col_psutzipcit`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
RENAME TABLE `tb_hovvshypke` TO `tb_zixkylluol`, `tb_tfimbaoajc` TO `tb_rmmpcdzkft`;
ALTER TABLE `tb_zixkylluol` ADD COLUMN `col_iwtkwazsiv` time NULL DEFAULT '00:00:00' AFTER `col_psutzipcit`;
ALTER TABLE `tb_zixkylluol` ADD COLUMN `col_cqbtbuqstu` smallint unsigned;
ALTER TABLE `tb_zixkylluol` DEFAULT CHARACTER SET utf8mb4;
ALTER TABLE `tb_zixkylluol` ALTER COLUMN `col_psutzipcit` DROP DEFAULT;
ALTER TABLE `tb_zixkylluol` ALTER `col_psutzipcit` DROP DEFAULT;
ALTER TABLE `tb_zixkylluol` DROP `col_cqbtbuqstu`, DROP `col_iwtkwazsiv`;
ALTER TABLE `tb_zixkylluol` DROP KEY `col_psutzipcit`;
