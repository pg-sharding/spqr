
select __spqr__console_execute('show distributions') /*__spqr__preferred_engine: v2 */;

select __spqr__console_execute('create distribution d column types int') /*__spqr__preferred_engine: v2 */;

select __spqr__console_execute('show distributions') /*__spqr__preferred_engine: v2 */;

select __spqr__console_execute('drop distribution all cascade') /*__spqr__preferred_engine: v2 */;