
SET __spqr__maintain_params TO TRUE;

SET allow_system_table_mods TO true;

CREATE EXTENSION spqrguard;

CREATE TABLE guard_zz(i INT) /* __spqr__auto_distribution: REPLICATED */;

SELECT spqr_metadata.mark_reference_relation('guard_zz') /* __spqr__execute_on: sh1 */;
SELECT spqr_metadata.mark_reference_relation('guard_zz') /* __spqr__execute_on: sh2 */;
SELECT spqr_metadata.mark_reference_relation('guard_zz') /* __spqr__execute_on: sh3 */;
SELECT spqr_metadata.mark_reference_relation('guard_zz') /* __spqr__execute_on: sh4 */;

INSERT INTO guard_zz (i) VALUES(1);

SET spqrguard.prevent_reference_table_modify TO on;

INSERT INTO guard_zz (i) VALUES(1);

DROP TABLE guard_zz;
DROP EXTENSION spqrguard;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
