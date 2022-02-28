# Project2
Revature Big Data Project 2

Team Lead - Samuel Taylor
Team Members - Mark Coffer, Jacob Nottingham, Mandeep Atwal

Project Requirements: Size of data should be 2k and above,and a minimum 3 tables. Your project should involve useful analysis of data(Every concept of spark like rdd, dataframes, sql, dataset and optimization methods and persistence should be included). The Expected output is different trends that you have observed as part of data collectivley and how you can use these trends to make some useful decisions. Should have admin and normal user access with password set in database along with Visualization for output. 

Our project uses a custom GUI built with ScalaFX.  Usernames and passwords stored using AWS Relational Database Service.  Program has admin and user level access with different features available for each.  We used Covid 19 Data supplied by the WHO and compiled by John's Hopkins University.  Our goal was to create an efficient, optimized program that performs multiple queries on our data quickly.  We saved our tables in parquet format which made them queryable without having to create a temporary table each time the program ran, which significantly sped up our queries.  We used Zeppelin to create graphs and charts for each query, saved the screenshots in png format and loaded them using the GUI to provide an interesting way to view them.
