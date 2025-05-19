-- Table created- SP_COMPARISON_QUERY_TABLE, SP_RESULT_TABLE
use schema analytics.SCD;
create or replace procedure sp_test(TABLE_NAME varchar, TABLE_NAME2 varchar, KEY_COLUMN varchar)
returns variant not null
language JAVASCRIPT
execute as caller
as
$$
try{
    var sql_command = `with columns as (
	select
	column_name
	from information_schema.columns
	where table_name = '${TABLE_NAME}'
	and table_schema = current_schema()
    and table_catalog = current_database()
)
,ct_comparison_query as ( 
select
	'create or replace table SP_COMPARISON_QUERY_TABLE as select t1.${KEY_COLUMN},' || 
	listagg(' case when t1.' || column_name ||' = t2.' || column_name || ' or (t1.' || column_name || ' is null and t2.' || column_name ||
	' is null) then ''Match'' else ''Mismatch'' end as '|| column_name || '_match'
	,',') within group(order by column_name) || ' from ${TABLE_NAME} t1 join ${TABLE_NAME2} t2' || ' on t1.${KEY_COLUMN} = t2.${KEY_COLUMN};' 
	as comparison_query
from columns
)
select
comparison_query as scripts
from ct_comparison_query
union all
select
	'create or replace table SP_RESULT_TABLE as \n select ${KEY_COLUMN}, COLUMN_NAMES from ('
	|| listagg('\n select ${KEY_COLUMN}, ''' || column_name || ''' as COLUMN_NAMES, ' || column_name || '_match as match_status from comparison_query where ' || column_name || '_match = ''Mismatch'''
	,' union all ') 
	 within group(order by column_name) ||
	' ) unpivoted_data\n ;' as scripts
from columns
;`;
    var stmt = snowflake.createStatement({sqlText: sql_command});
    var result = stmt.execute();
    var sql_command1, sql_command2;
    if(result.next()){
        sql_command1 = result.getColumnValue(1);
    }else{
        throw new Error("No data found");
    }
    snowflake.createStatement({sqlText: sql_command1}).execute();
    result.next()
    sql_command2 = result.getColumnValue(1);
    var res3 = snowflake.createStatement({sqlText: sql_command2}).execute();
    var sql_command3 = 'SELECT * FROM SP_RESULT_TABLE';
    var rt = snowflake.createStatement({sqlText: sql_command3}).execute();
    var output_array = [];
    while(rt.next()){
        var row = {};
        row[rt.getColumnName(1).toUpperCase()] = rt.getColumnValue(1)
        row[rt.getColumnName(2).toUpperCase()] = rt.getColumnValue(2)
        output_array.push(row);
    }
    return output_array;
}
catch(err){
    errArr = {'Error': err.message}; 
    var output_array = [];
    output_array.push(errArr);
    return output_array;
}
$$
;
call sp_test('DIM_STUDENT','DIM_STUDENT2','STUDENT_ID');
select * from comparison_query;
select * from result_table;
drop table if exists comparison_query;
drop table if exists result_table;