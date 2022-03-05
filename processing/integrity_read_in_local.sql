set session sql_mode = '';
set session sql_mode = 'NO_ENGINE_SUBSTITUTION';

delete from organic_integrity.organic_item;
delete from organic_integrity.organic_operation;

load data infile 'organic_operations.csv' into table organic_integrity.organic_operation fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;

load data infile 'organic_items.csv' into table organic_integrity.organic_item fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;

/* Below is no longer needed - fixed null issue in intgrity_xml_import*/
/*update organic_integrity.organic_item
set ci_nopCategory = NULL where 
ci_nopCategory = "";

update organic_integrity.organic_item
set ci_nopCatName = NULL where 
ci_nopCatName= "";

update organic_integrity.organic_item
set ci_itemList = NULL where 
ci_itemList = ""; */

