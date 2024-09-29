create table destination_transactions
(
  id                      integer      not null,
  transaction_date        date         not null,
  account_id              integer      not null,
  amount                  numeric      not null,
  created_at              date,
  constraint pk_destination_transactions primary key(id)
);

CREATE INDEX destination_transactions_account_id_idx ON destination_transactions(account_id) USING btree ;

create table destination_accounts
(
  id                      integer      not null,
  account_number          varchar(100)      not null,
  created_at              date,
  constraint pk_destination_accounts primary key(id)
);

create table destination_accounts_balance
(
  account_id              integer      not null,
  balance                 numeric      not null,
  constraint pk_destination_accounts_balance primary key(account_id)
);