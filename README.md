# ----------------------------------------------------------------------------
# Created By  : Lakshma Kasu
# Created Date: 01/04/2023
# version ='1.0'
# ----------------------------------------------------------------------------


Hello, Welcome to IMDB Sample Application.

**Data Source
**
The dataset you will use can be downloaded from IMDB https://datasets.imdbws.com/. Please note that this dataset is just an example, the application should be expected to handle billions of records.

https://www.imdb.com/interfaces/

 
**Problem Statement
**
 
Your task is to write a Spark application that can answer the following questions using the imdb data set.

Q1. Retrieve the top 20 movies with a minimum of 50 votes with the ranking determined by
(numVotes/averageNumberOfVotes) * averageRating

Below is the spark submit command to submit the spark application 

**spark-submit IMDBDataProcesser.py inputs\name_basics.tsv inputs\title_akas.tsv inputs\title_basics.tsv inputs\title_crew.tsv inputs\title_episode.tsv inputs\title_principals.tsv inputs\title_rating.tsv**


inputs\name_basics.tsv, other parameters -- Input file location, if we are passing from different location then, we have to pass the same value here 


STEP 1:-  Load the IMDB datasets into a Spark Data Frame
And performing argument length and file validation checks 

![image](https://user-images.githubusercontent.com/129509447/229279594-5b3e686b-5e24-4dd2-88a7-acae17316af5.png)

![image](https://user-images.githubusercontent.com/129509447/229279612-7cad23dc-aa1c-466b-9945-bb2c9cf450c9.png)



STEP 2:- Filter out movies with less than 50 votes.

![image](https://user-images.githubusercontent.com/129509447/229247823-fd0934e7-fac8-4015-a1bf-2ce3105c3643.png)

![image](https://user-images.githubusercontent.com/129509447/229247846-4cb7a67d-970e-4c04-8bd9-41bdcb8b4df5.png)


STEP 3:- Calculate the average rating and number of Average Number of Votes as below and 
Calculate the ranking score using the formula: (numVotes/averageNumberOfVotes) * averageRating.


![image](https://user-images.githubusercontent.com/129509447/229247891-f3b737bb-bffa-4649-96a6-0c8e6b2f5e3e.png)

![image](https://user-images.githubusercontent.com/129509447/229247976-7d1fd4f7-07c5-4472-9865-bddd44d3dad0.png)

![image](https://user-images.githubusercontent.com/129509447/229248034-96d84d4e-3b93-4a21-87b4-6c44c92e8cbc.png)



STEP 4:- Sort the movies based on their ranking score in descending order.

![image](https://user-images.githubusercontent.com/129509447/229248096-b1fe1586-104b-46fd-9dab-3fd96b258fee.png)


STEP 5:-  Select the top 20 movies and display their title, ranking score, average rating, and number of votes.

![image](https://user-images.githubusercontent.com/129509447/229248178-5d61cd66-01e4-46d2-929d-15ff1ba3d32e.png)

![image](https://user-images.githubusercontent.com/129509447/229248144-12c4ca71-0d94-4da1-85f9-668e255d82f2.png)



Q2. For these 20 movies, list the persons who are most often credited and
list the different titles of the 20 movies.

Below are the points generated results

STEP 1:- Extract the top 20 movies from the previous step.

![image](https://user-images.githubusercontent.com/129509447/229248282-92516e89-5bbc-451d-8df3-1217ec865856.png)

STEP 2:- Join the top 20 movies with the "principals" dataset to get the list of persons credited for each movie.

![image](https://user-images.githubusercontent.com/129509447/229248340-1b2fcf42-774c-4d2a-9aaf-f8b368812129.png)


STEP 3:- Count the number of times each person is credited across all top 20 movies.

![image](https://user-images.githubusercontent.com/129509447/229248364-7b95589d-5674-4fc4-97e7-c73e29f1201e.png)

![image](https://user-images.githubusercontent.com/129509447/229248389-0878cf40-54f5-4b58-94dd-fc7dc93b8b7d.png)

STEP 4:- Sort the persons based on the number of times they are credited in descending order.
          Display the top 10 persons who are most often credited.
          
 ![image](https://user-images.githubusercontent.com/129509447/229248460-e4921185-eca8-4495-8132-4735442833c9.png)

![image](https://user-images.githubusercontent.com/129509447/229248481-e5bc0a81-735d-4997-bd7a-501b3c13e2fd.png)


Question 2.1 :- Display the different titles of the 20 movies.

![image](https://user-images.githubusercontent.com/129509447/229248534-fea34c79-2c01-43f2-8110-8c90fad7d9b8.png)

![image](https://user-images.githubusercontent.com/129509447/229248558-445c4b50-1824-40a3-a225-fa67bc48350d.png)


You can run the below test class as well to check the result 
**IMDBDataProcesserTest.py**

To run the test class, please use the below link to get the results:- 

**python IMDBDataProcessesTest.py inputs\name_basics.tsv inputs\title_akas.tsv inputs\title_basics.tsv inputs\title_crew.tsv inputs\title_episode.tsv inputs\title_principals.tsv inputs\title_rating.tsv**

Please find the below screenshots for the test evidence, ran the two test cases successfully

**SCREENSHOT**

![image](https://user-images.githubusercontent.com/129509447/229285667-fb0be60b-86f1-4fcd-ae61-045822b553b3.png)



![image](https://user-images.githubusercontent.com/129509447/229250729-cec76132-a117-4ad8-9a7b-9e6e07ef1ac1.png)


![image](https://user-images.githubusercontent.com/129509447/229251209-908cbb00-8754-444e-af24-bf0006120749.png)















