# Learning-Docs
Learning Pyspark and AWS. These past weeks I learned pyspark and some AWS features. Here is the documentation of what I have learnt so far. Everything is this document only applies to Windows Operating Sytems.

## PySpark 
Pyspark is an interface for Spark Apache in Python. To have access to this interface, you need to download and install the lates versions of Java, Python(or Anaconda), Hadoop, and Apache. 
### Installation
1. Install Java if you don't have it yet:
[Here is the link to download Java:](https://www.java.com/download/ie_manual.jsp)
2. Install Python or anaconda if you don't have it yet:
[Check this link for Python:](https://www.python.org/downloads/) Or [this one for Anaconda](https://www.anaconda.com/products/individual).
Make sure to check 'add to path' while installing python. 
3. [Download Apache Spark](http://spark.apache.org/downloads.html). Unzip it in a folder in your local (C:)
4. Finally, download winutils.exe for Hadoop. [You can download it from here](https://github.com/cdarlint/winutils). Need to match with the apache spark release that you download previously. 
- After downloading and installing Java, Python, Apache, Hadoop you need to add them to the path in your local machine by editing the systems environment variables. 
Open the 'edit systems environment variables' panel and edit the path by adding these new bin paths. Like this: 
You can restart your computer after everything. 

### Another way of installation
If you already have Java and Python installed just type in the command prompt: `pip install pyspark`. You can encounter some issues if Apache is not available in your local machine. In that case follow the 3 and below. 

### Coding
I used notebooks in Vs code to do my projects. 
#### 1. Libraries 
Pyspark has a lot of libraries like Python. Some of them are requirements in order to start your work: `SparkSession` for dataframes(DFs) and `SparkContext` for RDD( Resilient Distributed Datasets). I focused my learning on dataframes but I also learnt how to execute some line of codes with RDDS. 
- To start any work always import the libraries: 
`from pyspark.sql.import SparkSession ` : will import SparkSession to start working with DFs
`from pyspark import SparkContext`: will import SparkContext to start working with RDDs
- Example of some libraries that I used :
 `from pyspark.sql.types import * `: will import all datatypes and fields if I want to create a schema with them
`from pyspark.sql.functions import* `: will import all functions which can be mathematical functions or other regular functions

#### 2. How to extract 
After importing the libraries needed for the work, I can start by calling the method getOrCreate() which will allow me to get an existing session from spark or to create a new session returned by the builder. 
`spark = SparkSession.builder.appName('Testing').getOrCreate()`
After calling this method, I can create a file-path
`file-path = '/data-file-location-in-the-local-machine'`. If the data file is not in your local machine, you don't need to create a file-path. 
`df= spark.read.format('format-of-the-data-file').options('depending on the format of the file, you will have different options).load(file-path)'`
- example 1: my data file is a csv file in my local machine
`df= spark.read.format('csv').option('header','true').load(file-path)`
- example 2: my data file is a text file in my local machine
`df= spark.read.format('txt').option('wholeText','false').load(file-path)`

If the data file is not in my local machine the process is a little bit different: For jdbc or odbc, no need of creating a file-path, but will need to add a lot of options to help find the data. I will need to have the driver to access the database, the url, username, password, and database name. They will be all added in the spark.read as options. 

#### 3. how to work on the data
After extracting the data there are some functions to call for data manipulation: 
- df.show(): it allows to display the dataFrame. show() needs to be added to everything in order to display the right output except for df.columns and df.printSchema().... I can also use df.toPandas() to display in pandas' s version
- df.printSchema(): it will print the variables and their data types
- df.columns: it will print the columns names
- df.select('column1-name', 'column2-name').show(): it selects a column or multiple columns and diplay the results. I can also do: df.select(df.column1-name, df.column2-name).show() 
- df.filter(): is like WHERE in SQL statement. It allows to find a specific result following the condition stated. df.filter('column-name==(a variable)')
- df.sample(): this allows to create a sample of the data. ex: df.sample(fraction = 0.1, withReplacement=False) on 10% without replacing the empty field
- df.withColumn(): this creates a new column. ex: df.withColumn('column-name', df.column1-name) creates new column by replicating the column1-name's content or values
- df.withColumnRenamed(): rename a column-name. ex: df.withColumnRenamed('new-name', 'old-name')
- df.count(): count the number of rows in the table
- df.groupBy(): operates like groupBy in sql. ex: df.groupBy('column-name').count() will groupby the column and do the count and display the result
- df.orderBy(): operates like orderBy in sql. It will order by desc or asc
- df.sort(): will sort the data. ex: df.sort('column-name)
- df.groupBy().agg(): allows to operates some calculation by aggregations such as average, sum or min/max. ex: df.groupBy('column-name).agg({'column1-name':'min'}).show()
- df.drop(): remove a column. ex: df.drop('column1-name).show() will remove column1-name from the table
- df.drop_duplicates(): will remove duplicates
- df.write.save(): wille write into a new file 
These are the functions I think are important to manipulate with the data. And also there are similar functions in python that I can use with pyspark. For example df.split() or df.fillna()
## AWS
