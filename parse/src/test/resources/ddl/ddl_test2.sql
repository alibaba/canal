CREATE TABLE yushitai_test.card_record (
	id bigint AUTO_INCREMENT,
	name varchar(32) DEFAULT NULL,
	alias varchar(32) DEFAULT NULL,
	INDEX index_name(name),
	CONSTRAINT pk_id PRIMARY KEY (id),
	UNIQUE uk_name (name,alias)
) AUTO_INCREMENT = 256
