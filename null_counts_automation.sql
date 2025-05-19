-- null counts
-- Table created-  SP_NULL_CHECKS
use schema analytics.scd;
create or replace procedure null_counts(TABLE_NAME varchar)
returns variant
language javascript
as
$$
try {
    var sql_command = `with columns as (
    select column_name
    from information_schema.columns
    where table_name = '${TABLE_NAME}'
    and table_schema = current_schema()
    and table_catalog = current_database()
)
select
'CREATE OR REPLACE TABLE SP_NULL_CHECKS AS SELECT ' || 
listagg('(100-(ROUND(COUNT(' || column_name || ')/COUNT(*),5)) *100) as ' || column_name || '_nulls', ', ') within group(order by column_name)
|| ' from ${TABLE_NAME};' as script
from columns
;`;
var stmt = snowflake.createStatement({sqlText: sql_command});
var result = stmt.execute();
var sql_command2 = 'SELECT * FROM SP_NULL_CHECKS;';
var stmt2 = snowflake.createStatement({sqlText: sql_command2});
var result2 = stmt2.execute();
var output = [];
while(result2.next()){
    for(var i = 1; i <= result2.getColumnCount(); i++){
        var column_name = result2.getColumnName(i);
        var column_value = result2.getColumnValue(i);
        var dict = {};
        dict[column_name] = column_value;
        output.push(dict);
    }
}
return output;
}
catch(err) {
    errArr = [{'Error': err.message}];
    return errArr;
}
$$
;
call null_counts('DIM_STUDENT2');
