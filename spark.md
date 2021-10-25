# Spark

## quick launch a spark notebook

```python
docker run -dit -v /host/dir:/home/jovyan/work -p 8889:8888 --name spark2 jupyter/pyspark-notebook:spark-3.2.0
```

## Cache vs persist

[link](https://sparkbyexamples.com/spark/spark-difference-between-cache-and-persist/)

cache() method default saves it to memory (MEMORY_ONLY) whereas persist() method is used to store it to the user-defined storage level.

1. cache, By default Spark filter result is not cached.
   For example, `df2 = df.where("age > 5")` if df is loaded from file, df2 will be much slower than df. 
   
    To speed it up, we can use `df2 = df.where("age > 5").cache()`
   

## Spark with LightGBM

[https://lightgbm.readthedocs.io/en/latest/Parallel-Learning-Guide.html](https://lightgbm.readthedocs.io/en/latest/Parallel-Learning-Guide.html)

## Use SQL

To use SQL with spark, we can registerTempTable.

For example:

```python
df_table1 = spark.read.parquet('/home/table.parquet')
df_table1.registerTempTable("table1")
spark.sql("select col1, col2 from table1 limit 5").show()

# You should get something like following
# +-------+----------+-----+
# | col1  | col2     | col3|
# +-------+----------+-----+
# | 1     |    2     |   3 |
# +-------+----------+-----+
```

### Use SQL in a cell directly

[https://notebook.community/LucaCanali/Miscellaneous/Pyspark_SQL_Magic_Jupyter/IPython_Pyspark_SQL_Magic](https://notebook.community/LucaCanali/Miscellaneous/Pyspark_SQL_Magic_Jupyter/IPython_Pyspark_SQL_Magic)

**Run the following at beginning of the cell,** 

**Don't need all of these, just pick up the functions you need.**

```python
from IPython.core.magic import register_line_cell_magic

# Configuration parameters
max_show_lines = 50         # Limit on the number of lines to show with %sql_show and %sql_display
detailed_explain = True     # Set to False if you want to see only the physical plan when running explain

@register_line_cell_magic
def sql(line, cell=None):
    "Return a Spark DataFrame for lazy evaluation of the sql. Use: %sql or %%sql"
    val = cell if cell is not None else line 
    return spark.sql(val)

@register_line_cell_magic
def sql_show(line, cell=None):
    "Execute sql and show the first max_show_lines lines. Use: %sql_show or %%sql_show"
    val = cell if cell is not None else line 
    return spark.sql(val).show(max_show_lines) 

@register_line_cell_magic
def sql_display(line, cell=None):
    """Execute sql and convert results to Pandas DataFrame for pretty display or further processing.
    Use: %sql_display or %%sql_display"""
    val = cell if cell is not None else line 
    return spark.sql(val).limit(max_show_lines).toPandas() 

@register_line_cell_magic
def sql_explain(line, cell=None):
    "Display the execution plan of the sql. Use: %sql_explain or %%sql_explain"
    val = cell if cell is not None else line 
    return spark.sql(val).explain(detailed_explain)
```

After running the code above, you can do the following

```python
df = %sql select * from employee
# df becomes the result spark dataframe
```

Or 

```python

%%sql_show
select * from employee
## result
+----+------+---------------+----------+------+
|  id|  name|          email|manager_id|dep_id|
+----+------+---------------+----------+------+
|1234|  John|  john@mail.com|      1236|    10|
|1235|  Mike|  mike@mail.com|      1237|    10|
+----+------+---------------+----------+------+
```

[PySpark Tutorial for Beginners: Learn with EXAMPLES](https://www.notion.so/PySpark-Tutorial-for-Beginners-Learn-with-EXAMPLES-94a9c1216c95472daf08b87f55fd6a23)

## Make a nullable column in spark

```python
# when creating a column with following, nullable=False
df2 = df2.withColumn("Home", f.lit("1"))

# when creating a column with following, nullable=True
df2 = df2.withColumn("Home", f.lit("1").cast(pyspark.sql.types.IntegerType()))
```
