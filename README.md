# Project 2
Revature Big Data Project 2

# Project Description
Project Requirements: Size of data should be 2k and above,and a minimum 3 tables. Your project should involve useful analysis of data(Every concept of spark like rdd, dataframes, sql, dataset and optimization methods and persistence should be included). The Expected output is different trends that you have observed as part of data collectively and how you can use these trends to make some useful decisions. Should have admin and normal user access with password set in database along with Visualization for output. 

Our project uses a custom GUI built with ScalaFX.  Usernames and passwords stored using AWS Relational Database Service.  Program has admin and user level access with different features available for each.  We used Covid 19 Data supplied by the WHO and compiled by John's Hopkins University.  Our goal was to create an efficient, optimized program that performs multiple queries on our data quickly.  We saved our tables in parquet format which made them queryable without having to create a temporary table each time the program ran, which significantly sped up our queries.  We used Zeppelin to create graphs and charts for each query, saved the screenshots in png format and loaded them using the GUI to provide an interesting way to view them.

# Technologies Used
-IntelliJ IDEA

-SBT v. 1.57

-Scala v. 2.11.12

-Spark v. 2.4.8

-ScalaFX v. 8.0.60-R9

-Spark MLIB v. 2.4.8

-Spark SQL v. 2.4.8

-Spark Hive v. 2.4.8


# Features
-Interactive GUI 

-Visualizations made in Zeppelin

-Optimization using Parquet files and caching

-Use of AWS RDS for storing username/passwords

# Getting Started
Repo can be cloned easily using the Github Desktop app or by using Intellij IDEA's create project from remote repository.  Because project was built in IntelliJ it includes multiple IDEA files which will prevent the project from working as is on other IDE's.


# Usage
Using the project will require creating/changing the default database used as it depends on AWS RDS for storing username/passwords.  However, this feature can be disabled to run the program without a database.  


# Contributors
-Team Lead - Samuel Taylor

-Team Members - Mark Coffer, Jacob Nottingham, Mandeep Atwal


# License
Unlicensed
