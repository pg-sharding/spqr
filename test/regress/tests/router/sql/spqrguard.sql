
SET __spqr__maintain_params TO TRUE;

SHOW spqrguard.prevent_distributed_table_modify /* __spqr__execute_on: sh1 */;
SHOW spqrguard.prevent_reference_table_modify /* __spqr__execute_on: sh1 */;

\c spqr-console
DROP DISTRIBUTION ALL CASCADE;
