drop table if exists source_accounts;
create table source_accounts
(
  id                      integer unsigned not null auto_increment,
  account_number          varchar(100) not null,
  created_at              date,
  constraint pk_source_accounts primary key(id)
);

drop procedure if exists insert_source_accounts;

delimiter //
create procedure insert_source_accounts()

begin

declare v_max int unsigned default 1000000;
declare v_counter int unsigned default 0;

  truncate table source_accounts;
  start transaction;
  while v_counter < v_max do
    INSERT INTO source_accounts (account_number, created_at) VALUES (MD5(RAND()), CURRENT_DATE - INTERVAL FLOOR(RAND() * 14) DAY);
    set v_counter=v_counter+1;
  end while;
  commit;
end;//

delimiter ;

call insert_source_accounts();