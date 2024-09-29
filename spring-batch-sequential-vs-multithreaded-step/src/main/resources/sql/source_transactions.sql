drop table if exists source_transactions;
create table source_transactions
(
  id                      integer unsigned 	not null auto_increment,
  transaction_date        date         		not null,
  account_id              integer      		not null,
  amount                  numeric      		not null,
  created_at              date,
  constraint pk_source_transactions primary key(id)
);

drop procedure if exists insert_source_transactions;

delimiter //
create procedure insert_source_transactions()

begin

declare v_max int unsigned default 1000000;
declare v_counter int unsigned default 0;

  truncate table source_transactions;
  start transaction;
  while v_counter < v_max do
    INSERT INTO source_transactions (transaction_date, account_id, amount, created_at) VALUES (CURRENT_DATE - INTERVAL FLOOR(RAND() * 14) DAY, FLOOR(RAND()*(1000000-0+1))+10, FLOOR(RAND()*(1000000-0+1))+10, CURRENT_DATE - INTERVAL FLOOR(RAND() * 14) DAY);
    set v_counter=v_counter+1;
  end while;
  commit;
end;//

delimiter ;

call insert_source_transactions();