# joiningtablesusinggluejob
Created a glue job using spark script. Where 2 cvs files customer.csv and product.csv read to two data frames.
Added 3 transformations,
1. Joined both the tables 
2. applied filter on the salary >=15000
3. transformed the schema by converting the fields with long to int.

finally the result is written to the output folder in S3 bucket.
