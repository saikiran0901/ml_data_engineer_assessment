# Code test for data engineers

## Purpose

This is a test to demonstrate your understanding of data pipelines, SQL databases, and ability to manipulate data into a format that is accessible for data scientists.

## Prerequisites

- Knowledge of python and the tools to integrate with APIs, process data, and interact with a file system and a SQL database.
- Knowledge of relational databases, including how to create tables, insert data, and query data. For the purpose of this test, we are using MySQL.
- Familiarity with Docker for container management, which we use through the Docker Compose tool. You will need Docker and Docker Compose installed on your development machine.
- Familiarity with Git for source control, and a github.com account which will be used for sharing your code.

We have included a test script that will ensure that the file system and MySQL database are set up correctly and accessible.

## Background

We have provided a Github repo containing:

- A **docker compose.yml** file that configures a container for the MySQL database and the script
- A **Dockerfile** to build and run the python script
- A **mysql-schemas** folder containing a test.sql file. You can add your sql schemas here.

## Test

To ensure the database is up and running, the following test can be run:

```
docker-compose up test
```

## Assessment

The assessment consists of a series of small tasks to demonstrate your ability to perform the role of a data engineer at Profasee. We will be looking for both your ability to complete the tasks as well as the tools, data structures, python features, and code structure you use to accomplish the final result. Any python package needed can be added to the requirements.txt file. All code should be added to the `assessments` folder and be able to be run with the following command:
```
docker-compose up assessment
```

Fork the git repo to your own Github account and complete the following tasks:

1. Download the CSV hosted at https://profasee-data-engineer-assessment-api.onrender.com/people.csv:
* Store the raw data in the `/data` directory.
* Convert the CSV to JSON format and store in the `/data` directory.
2. Inspect the data and list ways that the data can be cleaned up before being stored for a data science team to use.
* Write code to peform at least two types of the cleaning.
* Write unit tests to show the data cleaning functions work as expected.
* Write a function to filter people who have no interests.
3. Design a database schema to hold the data for the people in the CSV.
* Store the schema file in the `mysql-schemas` directory. These will be applied when the database container is created.
* Write code to load the data from the CSV into the database.
4. Create a function that uses the database tables to return the following stats of the people data:
* The minimum, maximum, and average age
* The city with the most people
* The top 5 most common interests
5. Bonus: Create an API that serves an endpoint to return the data in Task 4.


Share a link to the cloned github repo with the completed tasks so we can review your code ahead of your interview.