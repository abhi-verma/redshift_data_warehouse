# redshift_data_warehouse

### Introduction
A startup named Sparkify is a digital music streaming service that gives access to millions of songs and other content from artists all over the world. The company wants to analyze the users and songs data that it is currently collecting on their streaming app. The analytics team is interested in understanding what songs, genre or artists users are listening to, so that they can provide targeted marketing to increase the brand value and revenue of the company. The data currently resides in a Amazon S3 with a directory of JSON logs on user activity on the app, and a directoy of JSON metadata on the available songs on the app. The company is expecting a Data Engineer to process these logs and metadata files and store it in a AWS Redshift cluster optimized to provide song play analysis by using a dimensional model consisting of fact and dimension tables. This project connects to a AWS Redshift Cluster and creates the data warehouse using ETL pipelines.

### Project Dataset
The project uses two datasets, one for the songs metadata and other one for the user activity on the app. The datasets are stored on AWS S3.
- Songs Data: s3://udacity-dend/song_data
- Logs Data: s3://udacity-dend/log_data

### Data Model
The data model resembles a star schema with one Fact table, songplays and four dimension table: users, songs, artists and time. A star schema provides the advantages of higher query performance, built-in referential integrity and ease of understanding.
![Data Model](img/DataModelSparkify.png)

### Project Structure
- dwh.cfg: This is a configuration file while stores host name and other database parameters for the AWS Redshift cluster. It stores the arn (Amazon Resource Name) and the paths for the S3 buckets for songs and logs datasets. The parameters are referenced by other scripts in the project.
- sql_queries.py: This script consists of drop, create and insert statements for all the staging, fact and dimension tables. It is imported by other scripts to use the queries.
- create_tables.py: This script is used to drop and re-create staging, fact and dimension tables.
- etl.py: This script first copies the datasets into staging tables and then inserts data in fact and dimension tables referencing the staging tables.
- img folder: This contains the data model image used in this readme file.

### Project Steps
1. In a AWS account, create an IAM role with read access to S3. Copy the arn of the role and replace it in the dwh.cfg file.
2. Create a security group to open an incoming TCP port to access the cluster ednpoint.
3. Create a Redshift cluster and add the IAM role created earlier to this cluster. Under Network settings, choose the security group created above and change the publicly accessible setting to 'Yes' to allow instances and devices outside the VPC connect to your database through the cluster endpoint. Copy the cluster endpoint and database parameters and replace in dwh.cfg file.
4. Run the create_python.py file to drop and create the staging, fact and dimension tables.
5. Run the etl.py file to load the data from S3 to staging tables and then insert data into the fact and dimension tables. This step would take 5-6 minutes to complete.
6. Verify the tables and the data by using the Query Editor in the AWS Redshift Cluster console.
7. Delete the cluster, IAM role and secruity group.




