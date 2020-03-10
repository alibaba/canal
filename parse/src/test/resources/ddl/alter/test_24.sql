CREATE TABLE `tb_joiefnrthx` (
  `col_ctzpiugxrl` binary(180),
  UNIQUE INDEX `col_ctzpiugxrl` (`col_ctzpiugxrl`(6)),
  UNIQUE KEY `uk_xedljqlhxo` (`col_ctzpiugxrl`(18))
) DEFAULT CHARSET=latin1;
RENAME TABLE `tb_joiefnrthx` TO `tb_bubrykvvkb`;
RENAME TABLE `tb_bubrykvvkb` TO `tb_fywhebwffn`;
ALTER TABLE `tb_fywhebwffn` DEFAULT CHARACTER SET utf8mb4;
ALTER TABLE `tb_fywhebwffn` ADD UNIQUE KEY `uk_keugblaszl` (`col_ctzpiugxrl`(8));
ALTER TABLE `tb_fywhebwffn` ADD UNIQUE (`col_ctzpiugxrl`(13));
ALTER TABLE `tb_fywhebwffn` CHANGE `col_ctzpiugxrl` `col_ukjlmzjcdn` tinyint(91) unsigned DEFAULT '1';
ALTER TABLE `tb_fywhebwffn` DROP KEY `col_ctzpiugxrl`;
