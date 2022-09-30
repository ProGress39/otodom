Program allows orchestrated data web scrap from otodom and olx websites and store in MySQL database.

The purpose for this program is to capture current image of posts on both sites every week for further analyse. So we don't want to have current updated database
of available posts, but current situation captured every week. By doing so we can then analyse how market changed with time.

Technologies used:
1) Python
2) Beatifulsoup4
3) Pararell processing
4) Apache spark
5) Apache airflow
6) MySQL database

Everything simply described on project schema below. When Airflow starts it downloads the posts from both sites using python with bs4 and Apache Spark to save data
into dataframes and MySQL database. I know that it would be better to use pandas for this amount of data, but I wanted to try something more difficult just to learn.
After downloading it to MySQL database another program is runned. It reads data from MySQL database by Spark and make aggregations for 2 summary tables - one with
every city in project, another one with only main voivodeship cities.
Everything was done on linux, I also used MySQL in terminal to learn how to cooperate with it a bit.

<a href="https://imgbb.com/"><img src="https://i.ibb.co/qBhyC87/Untitled.jpg" alt="Untitled" border="0"></a>
