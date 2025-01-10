1. Calculate hottest and coolest month from ncdc data.
2. Execute following queries on "emp" and "dept" dataset.
    1. Create table "emp_staging" and load data from emp.csv in it.
    2. Create table "dept_staging" and load data from dept.csv in it.
    3. Display dept name and number of emps in each dept.
    4. Display emp name and his dept name.
    5. Display all emps (name, job, deptno) with their manager (name, job, deptno), who are not in their department.
    6. Display all manager names with list of all dept names (where they can work).
    8. Display job-wise total salary along with total salary of all employees.
    9. Display dept-wise total salary along with total salary of all employees.
    10. Display per dept job-wise total salary along with total salary of all employees.
    11. Display number of employees recruited per year in descending order of employee count.
    12. Display unique job roles who gets commission.
    13. Display dept name in which there is no employee (using sub-query).
    14. Display emp-name, dept-name, salary, total salary of that dept (using sub-query).
    15. Display all managers and presidents along with number of (immediate) subbordinates.
3. Execute following queries for books.csv dataset.
    1. Create table "books_staging" and load books.csv in it.
    2. Create table "books_orc" as transactional table.
    3. Create a materialized view for summary -- Subjectwise average book price.
    4. Display a report that shows subject and average price in descending order -- on materialized view.
    5. Create a new file newbooks.csv.
        ```
        20,Atlas Shrugged,Ayn Rand,Novel,723.90
        21,The Fountainhead,Ayn Rand,Novel,923.80
        22,The Archer,Paulo Cohelo,Novel,623.94
        23,The Alchemist,Paulo Cohelo,Novel,634.80
        ```
    6. Upload the file newbooks.csv into books_staging.
    7. Insert "new" records from books_staging into books_orc.
    8. Display a report that shows subject and average price in descending order -- on materialized view. -- Are new books visible in report?
    9. Rebuild the materialized view.
    10. Display a report that shows subject and average price in descending order -- on materialized view. -- Are new books visible in report?
    11. Increase price of all Java books by 10% in books_orc.
    12. Rebuild the materialized view.
    13. Display a report that shows subject and average price in descending order -- on materialized view. -- Are new price changes visible in report?
    14. Delete all Java books.
    15. Rebuild the materialized view.
    16. Display a report that shows subject and average price in descending order -- on materialized view. -- Are new price changes visible in report?
4. Upload busstops.json data into HDFS directory. Then create hive external table to fetch data using JsonSerDe.
    ```
    {"_id":{"$oid":"5a0720b478597fc11004d951"},"stop":"Non-BRTS","code":"103B-D-04","seq":4.0,"stage":1.0,"name":"Aranyeshwar Corner","location":{"type":"Point","coordinates":[73.857675,18.486381]}}
    ```
    ``` 
    location STRUCT<type:STRING, coordinates:ARRAY<DOUBLE>>
    ```
    ```
    When column-name have special charatcters like _ or $, they should be encapsulated in `back-quotes`.
    ```
5. Implement Movie recommendation system.
    * Example Input Data
        ```
        userId,movieId,rating,rtime
        17,70,3,0
        35,21,1,0
        49,19,2,0
        49,21,1,0
        49,70,4,0
        87,19,1,0
        87,21,2,0
        98,19,2,0
        ```
    * Create pairs of movies rated by same user.
        ```
        userId,movie1,rating1,movie2,rating2
        49,21,1.0,70,4.0
        49,19,2.0,21,1.0
        49,19,2.0,70,4.0
        87,19,1.0,21,2.0
        ```
    * Create correlation table.
        ```
        movie1,movie2,cnt,cor
        19,21,2,-1.0
        19,70,1,0.0
        21,70,1,0.0
        ```
    * Predict Similar movies for given movie Id. Get the recommended movies titles from movies table.
    * Hints
        * Start with above small data tables to test accuracy of the steps.
        * You will need to create new intermediate tables to store results of earlier queries.
        * For main data use ORC format to speed-up the queries.
        * You may need to change reducer tasks memory for quicker execution and avoid OutOfMemory errors.
            * SET mapreduce.reduce.memory.mb = 4096;
            * SET mapreduce.reduce.java.opts = -Xmx4096m;
