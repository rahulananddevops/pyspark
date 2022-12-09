# Shortcut/Workaround in Spark to avoid using repartitioning/shuffling /coalesce (like for example in Union operation which is too costly).
 Sometimes when the dataframes are too big and our APIs have some limitations in processing huge number of rows. In those case we might need to split our dataframes and then do the processing and then comes the part where we can join then which is very costly in spark if you go with Union. So here is a workaround in which we can use python’s basic data structures and do a union in our own way.

So we create a dataframe and use randomsplit to split in in 2 dataframes here.
 
Now we can perform some ETL or any other processing (with our APIS etc.) on the split dataframes and do a join in the following way using the dictionaries and list.
You can also rename the columns and do a join with the original dataframe if you want to add the column(choose your columns accordingly in the start).
