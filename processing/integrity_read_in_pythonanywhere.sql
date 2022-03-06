set session sql_mode = '';
set session sql_mode = 'NO_ENGINE_SUBSTITUTION';


load data local infile '/home/ddavis11/mysite/processing/organic_operations.csv' into table organic_operation fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;

load data local infile '/home/ddavis11/mysite/processing/organic_items.csv' into table organic_item fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
