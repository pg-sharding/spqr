
create distribution ds1 column types int hash;
create relation tr(MURMUR [id1 INT HASH, id2 VARCHAR HASH]);

CREATE KEY RANGE FROM 3221225472 ROUTE TO sh4;
CREATE KEY RANGE FROM 2147483648 ROUTE TO sh3;
CREATE KEY RANGE FROM 1073741824 ROUTE TO sh2;
CREATE KEY RANGE FROM 0 ROUTE TO sh1;

show relations;

DROP DISTRIBUTION ALL CASCADE;
DROP KEY RANGE ALL;
