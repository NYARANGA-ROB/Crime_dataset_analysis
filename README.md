Crime Rate Analysis is a group project was done by 



| Apache Spark | Scala | MapReduce |  Haddop | Hive | Cassandra |
| :------: | :------: | :------: |:------: | :------: | :------: |
| ![image1](https://github.com/Dhabbah/CSEE5590-490-Big-Data-Programming/blob/master/LAB2/Documentation/logo/1200px-Apache_Spark_Logo.svg.png) | ![image1](https://github.com/Dhabbah/CSEE5590-490-Big-Data-Programming/blob/master/LAB2/Documentation/logo/scala-logo.jpg) | ![image1](https://github.com/Dhabbah/CSEE5590-490-Big-Data-Programming/blob/master/LAB2/Documentation/logo/hadoop-logo.png) |  ![image1](https://raw.githubusercontent.com/Dhabbah/CSEE5590-490-Big-Data-Programming/master/LAB1/Documentation/logo/hadoop-logo.png) | ![image1](https://raw.githubusercontent.com/Dhabbah/CSEE5590-490-Big-Data-Programming/master/LAB1/Documentation/logo/1200px-Apache_Hive_logo.svg.png) | ![image1](https://upload.wikimedia.org/wikipedia/commons/thumb/5/5e/Cassandra_logo.svg/2000px-Cassandra_logo.svg.png) |



# Introduction

The purpose of this report is to illustrate what I have implemented for this project. This project aims to utilize Hadoop stack and Apache Spark in order to analyze and visualize information about crimes occurred in Kansas City. This information I have is from open data for Kansas City. The project is divided into three sections. Firstly,    I collect and clean the data I have. Secondly, I use big data tools, such as Hive to analyze it. Additionally, I will use SparkContext in Apache Spark in order to perform analysis and analytics. For the analytics part, I will use some algorithms in machine learning. Finally,I will show my implementation in a dashboard.

# Background

I am rewriting the background information here to avoid plagiarism from my previous submission. I have KC crime data, which presents a significant big data challenge as it involves handling over 10,000 crime reports. I am writing Python scripts and adapting them to Hadoop and PySpark implementations for a faster, more efficient solution. My scripts will be capable of producing useful insights and analysis in real-time using big data programming.

The big data approach I have chosen is carefully considered since I initially performed all these operations using basic Python and DataFrames for preliminary analysis. However, the massive size of the dataset made it nearly impossible for standard scripts to deliver real-time results. Therefore, I adapted the solution to a big data infrastructure to achieve real-time analysis and apply the concepts I learned in class..

# Related Work

I have not been able to get any help with this project because I could not find any resources relevant to this specific task. The work I am doing is unique, as there is no related work available for this specific dataset and type of analysis. When I search online for similar projects or guidance, I am unable to find anything applicable, and no prewritten scripts exist for this purpose. All the scripts I submit with this work are entirely my own and can be verified through plagiarism detection software. 

# Architecture Diagram and Workflow Diagram

![image](Documentation/images/1.PNG)

# Machine Learning 

 Machine Learning involves using algorithms to enable a system to learn from a given dataset. I have used Python to implement these algorithms and then applied PySpark to run the same algorithms, which I aim to illustrate in this report. I found that executing machine learning algorithms in PySpark significantly reduces training time. I used various algorithms, including Random Forest, to train my model, achieving good accuracy.

Training the model involved several steps. First, I performed feature engineering to prepare the data for the algorithms. Next, I split the crime dataset into two parts: one for training and the other for testing. Then, I trained the model using the training portion and subsequently tested its performance with the testing portion.

![image](Documentation/images/2.PNG)

# Dataset

The dataset includes yearly bases data for Crime reporting. We used this dataset for both the analysis and analytics.
- I have various features such as:
1. Reported year
2. Type of crime
3. Involvement
4. Race
5. FireArms used
6. Gender etc.

## Detailed Description

I have used open data to obtain my datasets. Initially, I worked with a dataset from the year 2018, but I later expanded it by collecting datasets from 2010 to 2018 for a broader analysis. The datasets provided various details about crimes, such as the type of crime, locations where they occurred, as well as demographic information like gender and race.

At first, my dataset contained 128,938 offenses since it was limited to 2018 data. However, after expanding the dataset, it grew to over one million records, specifically 1,121,574. To clean the dataset initially, I used Excel pivoting techniques. Later, I transitioned to using Python for data cleaning and preparation.

## Detail Design of Features

I had a goal that we implement our knowledge in analytics and analysis.

### For the dataset

- I used Python to load the datasets and merge them then we do feature engineering due to the variety of libraries that Python has. 

- I implemented some of our knowledge about Hadoop tools and Spark.

### For the visualization

- I used matplotlib to visualize the information.

- I also developed a simple dashboard using Angular and flask to illustrate the visualization.

![image](Documentation/images/3.PNG)

# Analysis of data

This dataset has more than a million records as shown below. 

![image](Documentation/images/4.png)

We have used different ways to feature engineering the dataset. Firstly, removing the features. There were some minor features that were not important and had more than 50% of the data missing; we removed them by getting rid of the columns. Secondly, there were some features had obvious values, such as City. Because the crimes occurred in Kansas City, we completed the missing values of the feature. Third, using a unique category. For example, There are only three types of values in gender, which are male, female and unknown.  Therefore, we filled the null values with the unknown category. Fourthly, We used the mean function to other missing values like the age. Finally, we removed the rest because they were few.

## Hive

Using Hive, a tool used in Hadoop stack, give us more insight into our data. Some of the implementation we have done, it shows below:

1. Find the number of crime types that happened in Kansas City.
2. Find how many crimes were in 2016 and 2018 based on males’ and females’ involvement.
3. Find how many people were arrested in the Plaza Area.
4. Find how many victims were in 2016.
5. Find how many suspects based on their race.

# Analysis Implementation

## Hive implementation

We used two portions here. First, we created a table and loaded the dataset to it. Second, we implemented the quires.

### Part 1:

We created the table for the crime dataset using Hive.

![image](Documentation/images/5.png)

Then, we loaded the dataset.

![image](Documentation/images/6.png)

### Part 2:

**First**, illustrating the number of crimes based on their types.

![image](Documentation/images/7.png)

**Result**

![image](Documentation/images/8.png)

![image](Documentation/images/v1.PNG)

**Second**, illustrating how many people were involved based on their gender in 2016 and 2018.

![image](Documentation/images/9.png)

**Result**

![image](Documentation/images/10.png)

![image](Documentation/images/v2-1.PNG)

![image](Documentation/images/v2-2.PNG)

**Third**, illustrating the number of people who were arrested in the Plaza Area.

![image](Documentation/images/11.png)

**Result**

![image](Documentation/images/12.png)

![image](Documentation/images/v3.PNG)

**Fourth**, illustrating the number of victims in 2016.

![image](Documentation/images/13.png)

**Result**

![image](Documentation/images/14.png)

![image](Documentation/images/v4.PNG)

**Fifth**, illustrating the number of crimes that people were suspected based on their race.

![image](Documentation/images/15.png)

**Result**

![image](Documentation/images/16.png)

![image](Documentation/images/v5.PNG)


## Pyspark Implementation

We have implemented following in Python functions and then called those functions using Pyspark User Defined Functions or UDF as you may.
We performed Analysis on the Data using the robust and speeded up operations via Pyspark by writing a function in plain python and then calling it via UDF.

- We can get crimes in a certain Radius
- We can get Crimes in a certain time frame
- We also combined the above two together.
- We get number of a specific type of crimes in a certain radius
- We get all different type of crimes that happened in general.
- We have been able to implement a safety index check

**Code for Radius Calculation!**

![image](Documentation/images/17.png)

Functions are written in python but later called using Pyspark UDF.
UDF maps any kind of function or class to Pyspark implementation without having to modify the code from scratch.

When we run this code, we are prompted to enter the filters. Such as Radius, ZipCode etc:

![image](Documentation/images/18.png)

We decided to stick to zipcode and not long lat coordinates because they are consistent whereas any other form of location data is subject to redundancy and change.


**Result:**

![image](Documentation/images/19.png)

**Radius as 3 Miles and Zipcode 64119**

Now, this is getting me data for the past 3 months, I can change that and get much more data. As shown below!

**Result:**

![image](Documentation/images/20.png)

Code for the function that we called using UDF

![image](Documentation/images/21.png)

We now implemented the threshold value for safety spike in the code. We integrated it with the types of crimes and based on these we are finally able to get safety level information to customers for a certain location.

We can check all different type of crimes and their frequency using below code:


![image](Documentation/images/22.png)

Result for this code will be an option to the User where they are free to get statistical results for a certain data or are they want to get it for all the crimes.

![image](Documentation/images/23.png)

Now our user will select a crime here or enter x as default for all types.
Once that is done, the code propagates into the next stage. Where it asks the user for the Radius of the area in which the statistical data needs to be generated.
And giving the ZipCode here like below:


![image](Documentation/images/24.png)


We now have results start pouring in on the console as below:

![image](Documentation/images/25.png)

We can see a few details in all these returning statements:
- We can see the distance from the epicenter of crime
- We can see the date on which the crime happened
- We can see the count of personal and property crimes that happened until that day.


We created a Peace Index for the Crime Data.
The whole Safety level was gauged by careful analysis of human nature and the crime data that we have.
We are able to confidently say that Humans feel danger or an attack on their safety in the presence of a crime that directly affects humans personally. Such as Assault, Rape and Hit and Run, etc.
Humans have a lasting traumatic and concussion experience when such an incident happens and the effect lasts up to 70% more than that of a non-personal attempt.
Using this calculation we are able to get a percentage of the total crimes that happened where Humans were involved and where only property damage was involved.
Using this we are very confidently able to report the safety or danger level in a certain radius of the point of interest. Our Safety index hence is an aggregation of 70% of crimes against humans and 30% of crimes against property or anything where humans were not directly affected by it.
This resulted in a very accurate depiction of safety levels.
Below is the Safety index report of <=1000 crimes that happened at 64112 in the last 3 months in a radius of 6 miles:

![image](Documentation/images/26.png)

We get a Safety level of 60% for this location by looking at the past 6 months worth of data.
Let us check the same for the past 3 months of data!
Here we get 61%

![image](Documentation/images/27.png)

The Complete Code for this implementation is below:

![image](Documentation/images/28.png)

The Dataset on which this was implemented is Below and will be provided in the Github Repository!


![image](Documentation/images/29.png)


## Cassandra CQL Analysis

We also performed CQL Queries on our Dataset to get some valuable insight into the data.
We first created a Workspace in Cassandra:

![image](Documentation/images/c1.png)

Then we created a Table for the Massive Data:


![image](Documentation/images/c2.png)

We then copy our data into this table:


![image](Documentation/images/c3.png)

After we are done, we visualize our data to see if we have the table ready:


![image](Documentation/images/c4.png)

Now comes the analysis:
We can get all reportings for a specific type of crime: Say 501

![image](Documentation/images/c5.png)

We can count the type of crimes:


![image](Documentation/images/c6.png)

We can also see the total number of arrests!

![image](Documentation/images/c7.png)

Cassandra is very fast and gives us good and quick enough analysis of the huge data and lives up to its good name


## Pyspark SQLContext

The time analysis (Year, Month, Week) is done through Pyspark SQL. To elaborate on the time analysis we can focus on the following points.

1. How many crimes have occurred every year.
2. Is the number of crimes increased or decreased in the last decade.
3. In which month the number of crimes is more per year.
4. What is the frequency of crimes at specific days of the week.
5. Are the crimes more during the weekend or during the weekdays.

To answer the above questions Pyspark SQL is very helpful. These questions sound very important but on the other hand the answer to these questions can easily be achieved by applying simple queries. 



![image](Documentation/images/30.png)

The above screenshot shows the months in which the maximum number of crimes have occurred in each year.

![image](Documentation/images/31.png)

The above screenshot shows the occurrence of a maximum category of crime.
In order to answer the day in which the crime is maximum. We have applied the following query which answers our question.

![image](Documentation/images/32.png)


The above query shows us the number of crimes that have occurred at every day of the week per year.


The above analysis can help us to answer any questions about the crime data at which year or month or even which day of the week the crime is more or which crime is more.


### PySpark Streaming

Pyspark Streaming solves the big problem of dividing our data into different batches according to year and category of the crime and do the analysis or processing on it individually. In the project, we have used pyspark text streaming to generate batches of our main file into years with one type of crime in each batch.
After generating these batches we read the file through our text stream and generate a dataframe from it. After generating the dataframe we analyze different properties of crime per year and save it in an individual text file to make it user-friendly for the other people.


![image](Documentation/images/33.png)

![image](/Documentation/images/34.png)

![image](Documentation/images/35.png)

In the first screenshot, we see that batches of CSV files are created. After that, each CSV file is read through text stream and then a text file is made the contents of which can be shown in the last screenshot.

Below is the system level diagram of it.

![image](Documentation/images/36.png)

Below are some previous queries with a simple analysis.	

What is the total number of crimes per year

![image](Documentation/images/37.jpg)


Counting the females who have been the victims of crimes.

![image](Documentation/images/38.jpg)

Analysis of crime which involved minors.

![image](Documentation/images/39.jpg)

Average analysis of crimes per year

![image](Documentation/images/40.jpg)


# Analytics Implementation

One of our implementations is the analytics part. We have done some machine learning algorithms on our dataset. For instance, Random Forest, which is for classification, was used to train our model. We also performed Naive Bayes and implemented Decision tree, which gave us better results.
We first imported the libraries we needed and loaded the dataset in SparkContext.

![image](Documentation/images/41.png)

Then we used vector assembler for feature columns.

![image](Documentation/images/42.png)

Next, we split the dataset into to parts, 70% for training and 30% for testing.

![image](Documentation/images/43.png)

Finally, we have implemented the following algorithms:

**1. Random Forest**

![image](Documentation/images/44.png)

**2. Naive Bayes**

![image](https://github.com/Dhabbah/Crime-Rate-Analysis/raw/master/Documentation/images/45.png)

**3. Decision Tree**

![image](Documentation/images/46.png)

We found out that Decision Tree gave use more accuracy than the rest while we were expecting to have better accuracy using Random Forest.

### Simple Visualization on Flask

Some Simple visualization code has been implemented on flask which shows the locations of crimes on google maps. 

![image](Documentation/images/47.png)

![image](Documentation/images/48.png)

**Result** 

![image](/Documentation/images/49.png)

![image](Documentation/images/50.png)

![image](Documentation/images/51.png)


# Conclusion
To conclude, this project has many phases starting with cleaning the dataset and ending with doing analysis and analytics. For Hive,  SQL context, and the time analysis, we can say that a lot of answers can be achieved by only doing the yearly, monthly and weekly analysis. Simple queries can help us achieve major milestones which can not only help us take precautions regarding those crimes but also help us to eradicate crimes using these data. In addition, by training a model with different algorithms, it gave us a better choice for the best algorithm. Then  we were able to predict the crime that firearms were used

# Future Work

For future perspective, we have data that focuses on the time at which the crime has occurred. The analysis of these times of the day can help us to predict and analyze which part of the day the crime rate is more. 
Spark Stream helps us to achieve fast processing. In our project spark streaming has helped us to develop different data sets which can not only be used by other data scientist but also help us to focus only on specific crimes rather than on whole datasets.

# Issues and Concerns

Some of the issues which we faced during the development of our projects are:

- The project in itself is really big having multiple applications in it. It is really hard to choose which application to pick and which to skip and most of them are integrated with each other.

- The datasets are picked from sites which are mostly consistent but small inconsistencies created big issues when loading in the code for generating tables and queries.

- Time constraint is another issue which has caused us to skip multiple analysis which could have been done to increase the efficiency of our work.

- Integration of multiple codes together involving machine learning and other pyspark modules.


# References

[1]. KCPD Crime Data 2018: https://data.kcmo.org/Crime/KCPD-Crime-Data-2018/dmjw-d28i/data.

[2]. Pandas.DataFrame.dropna: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.dropna.html

[3]. Drop Rows with NAN/ NA: http://www.datasciencemadesimple.com/drop-rows-with-nan-na-drop-missing-value-in-pandas-python-2/

[4]. Pandas.DatetimeIndex: https://pandas.pydata.org/pandas-docs/version/0.23.4/generated/pandas.DatetimeIndex.html

[5]. Pivot Tables: https://www.excel-easy.com/data-analysis/pivot-tables.html

[6]. Apache-hive: https://mapr.com/products/apache-hive/

[7]. Using Hive to perform some queries: https://github.com/Dhabbah/CSEE5590-490-Big-Data-Programming/wiki/ICP4

[8]. Numpy queries on Dataframes. https://docs.scipy.org/doc/numpy/reference/arrays.indexing.html

[9]. DSTREAMS examples https://gokhanatil.com/2018/04/pyspark-examples-5-discretized-streams-dstreams.html

[10]. Dataframe and RDDs https://medium.com/@xuweimdm/how-to-make-a-dataframe-from-rdd-in-pyspark-c34f2888ea8

[11]. Pyspark SQL modules. https://spark.apache.org/docs/2.1.0/api/python/pyspark.sql.html

[12]. User Defined Functions . https://docs.databricks.com/spark/latest/spark-sql/udf-python.html

[13]. SQL Aggregate functions. https://www.w3resource.com/sql/aggregate-functions/Max-with-group-by.php

[14]. New Column operations on dataframes. https://stackoverflow.com/questions/33681487/how-do-i-add-a-new-column-to-a-spark-dataframe-using-pyspark

[15]. Conversion of dataframe to python lists. https://stackoverflow.com/questions/38610559/convert-spark-dataframe-column-to-python-list

