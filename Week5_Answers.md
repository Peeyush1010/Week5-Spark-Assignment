\# Week 5 Spark Assignment - Questions and Answers



\## Q1: What are the key limitations of traditional MapReduce that make Spark a preferred choice for modern big data processing?



\### Answer:



MapReduce writes intermediate results to disk after every stage, which makes processing slower. Spark uses in-memory processing, reducing disk I/O and significantly improving performance. Spark also supports real-time analytics, machine learning, SQL, and streaming workloads.



\---



\## Q2: Explain how Spark uses In-Memory Computing to speed up iterative machine learning algorithms compared to disk-based systems.



\### Answer:



Spark stores intermediate data in memory (RAM) instead of repeatedly reading and writing to disk. This allows iterative machine learning algorithms to access data much faster, resulting in improved performance.



\---



\## Q3: Write a code snippet to remove all duplicate rows from a DataFrame.



```python

df\_no\_duplicates = df.dropDuplicates()

df\_no\_duplicates.show()

```



\---



\## Q4: Write a query to filter data and group by a column to find average values.



```python

df.filter(df.Contract == "Month-to-month") \\

&#x20; .groupBy("InternetService") \\

&#x20; .avg("MonthlyCharges") \\

&#x20; .show()

```



\---



\## Q5: What is the difference between .na.drop() and .na.fill()? Provide an example.



\### Answer:



\* `.na.drop()` removes rows containing null values.

\* `.na.fill()` replaces null values with a specified value.



```python

df.na.fill("Unknown").show()

```



\---



\## Q6: Write a query to find the total count of records for each category where count is greater than 100.



```python

from pyspark.sql.functions import count



df.groupBy("InternetService") \\

&#x20; .agg(count("\*").alias("Total\_Customers")) \\

&#x20; .filter("Total\_Customers > 100") \\

&#x20; .show()

```



\---



\## Q7: How does the immutability of Spark DataFrames affect data cleaning operations?



\### Answer:



Spark DataFrames are immutable, meaning they cannot be modified directly. Operations such as dropping columns or renaming columns create a new DataFrame rather than changing the original one.



\---



\## Q8: Write a Spark command to filter records within a range and a condition.



```python

df.filter(

&#x20;   (df.tenure >= 18) \&

&#x20;   (df.tenure <= 30) \&

&#x20;   (df.Contract == "Month-to-month")

).show()

```



\---



\## Q9: Why is it important to handle null values before aggregation operations?



\### Answer:



Null values can affect calculations and lead to inaccurate results. Handling null values before performing aggregations improves data quality and reliability.



\---



\## Q10: Write code to cast a column and rename it.



```python

from pyspark.sql.functions import col



df\_new = df.withColumn(

&#x20;   "TotalChargesDouble",

&#x20;   col("TotalCharges").cast("double")

)



df\_new.printSchema()

```



\---



\## Q11: Explain the Shuffle process in Spark.



\### Answer:



Shuffle is the process of redistributing data across partitions during operations such as groupBy and join. It is considered a wide transformation because data moves between different executors and partitions.



\---



\## Q12: Write a code snippet to remove rows with null values or empty strings.



```python

df\_clean = df.filter(

&#x20;   (df.TotalCharges.isNotNull()) \&

&#x20;   (df.gender != "")

)



df\_clean.show()

```



\---



\## Q13: How do you use the .agg() function to calculate multiple statistics at once?



```python

from pyspark.sql.functions import min, max, avg



df.agg(

&#x20;   min("MonthlyCharges").alias("Minimum\_Charge"),

&#x20;   max("MonthlyCharges").alias("Maximum\_Charge"),

&#x20;   avg("MonthlyCharges").alias("Average\_Charge")

).show()

```



\---



\## Q14: What is the risk of using inferSchema=True when source data contains inconsistent formats?



\### Answer:



Using `inferSchema=True` on inconsistent data may result in incorrect data type detection, leading to null values, schema mismatches, and processing errors.



\---



\## Q15: Write a final processing pipeline.



```python

from pyspark.sql.functions import avg



final\_df = (

&#x20;   df.dropDuplicates()

&#x20;     .na.fill("Unknown")

&#x20;     .groupBy("Contract")

&#x20;     .agg(avg("MonthlyCharges").alias("Average\_Charges"))

)



final\_df.show()

```



\### Output:



The final pipeline removes duplicates, handles missing values, groups data by contract type, and calculates average monthly charges.



