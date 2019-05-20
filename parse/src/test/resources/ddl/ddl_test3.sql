create table quniya4(name varchar(255) null,value varchar(255) null,id int not null,constraint quniya4_pk primary key (id));
alter table quniya4 modify id int not null first;
alter table quniya4 modify id int auto_increment;