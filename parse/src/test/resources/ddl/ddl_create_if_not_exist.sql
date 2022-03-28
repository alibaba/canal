CREATE TABLE `table_x1` (
    `id` bigint(20) NOT NULL AUTO_INCREMENT,
    `key1` longtext NOT NULL COMMENT 'key1',
    `value1` longtext NOT NULL COMMENT 'value1',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `table_x1` (
    `id` bigint(20) NOT NULL AUTO_INCREMENT,
    `key1` longtext NOT NULL COMMENT 'key1',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;