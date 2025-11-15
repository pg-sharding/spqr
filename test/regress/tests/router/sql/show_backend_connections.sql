\c spqr-console

create distribution ds1 column types integer;
alter distribution ds1 attach relation test distribution key id;
create key range from 1000 route to sh4 for distribution ds1;
create key range from 100 route to sh3 for distribution ds1;
create key range from 10 route to sh2 for distribution ds1;
create key range from 0 route to sh1 for distribution ds1;

\c regress

create table test(id int) /* target-session-attrs: read-write */;
select * from test /* target-session-attrs: read-only */;

\c spqr-console

SHOW backend_connections GROUP BY user;

SHOW backend_connections GROUP BY hostname;

SHOW backend_connections GROUP BY unknown;

SHOW backend_connections GROUP BY user, hostname;

SHOW backend_connections GROUP BY user, unknown;

\c regress

drop table test;

\c spqr-console

DROP DISTRIBUTION ALL CASCADE;
DROP KEY RANGE ALL;