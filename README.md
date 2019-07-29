# Tiki Scraping

## Introduction

**[Tiki.VN](https://tiki.vn/)** was founded by Son Ngoc Thai Tran in March 2010, at a time when the Vietnamese market was still new to E-Commerce. After 9 years of growing, it is currently one of the biggest online shopping platform in Vietnam.

**Tiki Scraping project** is the project that we finished after **the 2nd week** of studying **Machine Learning**.

**Project goals:**

**1. Crawling all products from Tiki with user ratings and comments**

**2. Pushing the data to a PostgreSQL database**

**3. Analyzing the data to get more insights of Tiki's business**

## Project Plan

* Install PostgreSQL
* Create Database `tiki` and Create tables for `Users`, `Products`, `Categories`, `Comments` in `db_init.py` (We currently have `Categories` and `Products` tables; `Users` and `Comments` tables belong to further work).
* Move solution of Week 1 project to a Python script `tiki_scraping.py` 
* Improve week 1 solution to get more info of the products
* Write functions to save `product` into the database 
* Analyze the data on Jupyter Notebook
* Implement the complete solution
* Push the solution to a new Github repo

## I. Install PostgreSQL
PostgreSQL is a free and open-source relational database management system. Using a database instead of regular spreadsheet will allow us to store a larger amount of data as well as offer a much higher capacity to later process our data. 

### Installation on Window
* [This link](http://www.postgresqltutorial.com/install-postgresql/) will give you the detailed description of the installation for Window.

### Installation on Linux 
* Step1: You can download the Post greSql source code from [this link](https://www.postgresql.org/download/) choose the mirror site that is located in your country & Install the Postgre SQL using commands
```
# tar xvfz postgresql-8.3.7.tar.gz 
cd postgresql-8.3.7
./configure
```


* Step 2: After the installation, make sure `bin`, `doc`, `include`, `lib`, `man` and share directories are created under the default `/usr/local/pgsql` directory 
```
ls -l /usr/local/pgsql/
```

*  Step 3: Create postgreSQL user account and data directory
* Step 4: Intialize the postgreSQL data directory & Validate it
* Step 8: Start postgreSQL database
* Step 9: Create postgreSQL DB and test the installation
* For more detailed steps please refer [this link](https://www.thegeekstuff.com/2009/04/linux-postgresql-install-and-configure-from-source/).

#### Example: On Ubuntu 18.04
* Step 1: Ubuntu's default repositories contain Postgres packages, so you can install these using the `apt` packaging system. Since this is your first time using `apt` in this session, refresh your local package index.
 
* Step 2: Install the Postgres package along with a -contrib package that adds some additional utilities and functionality using below commands:
```
$ sudo apt update
$ sudo apt install postgresql postgresql-contrib
```

### Installation of PostgreSQL on Mac OSX
* Step 1: Getting Homebrew : Install PostgreSQL on the command line we will be using a package manager called [homebrew](https://brew.sh/)
* Step 2: Install the Postgre
  Run the following command to install PostgreSQL using Homebrew:
```
brew install postgresql
```
* Step 3: Let’s go ahead and start Postgres running, and make sure Postgres starts every time your computer starts up. Execute the following command:
```
pg_ctl -D /usr/local/var/postgres start && brew services start postgresql
```

* Step 4: Finally, you can make sure Postgres is installed, running and check what version it is by giving following command:
```
postgres -V
```
* For more detailed information you can refer [this link](https://www.codementor.io/engineerapart/getting-started-with-postgresql-on-mac-osx-are8jcopb)

## II. Create Database & Create Table
### 1. Create a connection to PostgreSQL database server through command prompt 
The following command connects to a database under a specific user. After pressing Enter PostgreSQL will ask for the password of the user.
```
psql -d *databasename* -U *usersname*
```
<img src="https://i.imgur.com/ZGfNUUH.png" alt="drawing" width="400"/>

If you want to enable external access to a database that resides on another host, you add the `-h` option as follows:
```
psql -h <host> -p <port> -U <username> -W <password> <database>
```
`-h`: allows you to change IP address of the targeted computer.
`<host>`: Server's IP address on which postgres is to listen for TCP/IP connections from client applications.
`<port>`: Specifies the TCP/IP port or local Unix domain socket file extension on which postgres is to listen for connections from client applications (normally 5432).

|Commands | Description | 
| -------- | -------- | 
|\\?          |Know all available psql commands| 
| h ALTER TABLE   |Get help on specific PostgreSQL statement|
|\c dbname username  |Switch the connection to a new database|
|\l   | List all databases|
|\dt   | List all tables
|\d tablename|Describe a table such as a column, type, modifiers of columns, etc.|
|\du|List all users and their assign roles|
|\timing|Turn on or off query execution time|
|\e|Edit command in your own (favorite) editor|
|\a|Command switches from aligned to non-aligned column output|
|\H|Command formats the output to HTML format|
|\q|Quit psql|

### 2. Create a connection to Postgres from Python
* Connect to a database using psycopg2
```python
import psycopg2
def db_get_connection():
    # Input your username, password, host's number, port and
    # and the created database
    connection = psycopg2.connect(user = "yourusername",
                                  password = 'yourpassword',
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "tiki")
    return connection
    #test the connection
get_connection() 
```
***In Python, there is an open source library called [psycopg2](http://initd.org/psycopg/) that implements the Postgres protocol to connect to our Postgres server. You can think of psycopg2 being similar to connecting to a SQLite database using the [sqlite3](https://docs.python.org/3.5/library/sqlite3.html) library.***

### 3. Create table in PostgreSQL Database
* Create an empty table in our local PostgreSQL
* Cursor allows Python code to execute PostgreSQL command during our session (connection).
* Cursor encapsulates the query, and then read the query result a few rows at a time.
* *Advantage:* 
     * Avoid memory overrun when the result contains a large number of rows.
     *  Return a reference to a cursor that a function has created, allowing the caller to read the rows.
```python 
def create_tables():
    """ Create tables categories, products
    """
    try:
        connection = db_get_connection()
        if not connection:
            print('ERROR: Connection db fail')
            return
        cur = connection.cursor()

        query = """
			CREATE TABLE IF NOT EXISTS categories (
				category_id SERIAL NOT NULL PRIMARY KEY,
				name VARCHAR(200) NOT NULL,
				url VARCHAR(200) NOT NULL,
                parent INTEGER,
                weight FLOAT,
                count INTEGER,
				created_on TIMESTAMP
			);
		"""
        cur.execute(query)

        query = """
			CREATE TABLE IF NOT EXISTS products (
				product_id SERIAL NOT NULL PRIMARY KEY,
				category_id_fkey INTEGER REFERENCES categories(category_id),
                data_id VARCHAR(100) NOT NULL,
                seller_id VARCHAR(100),
				title TEXT NOT NULL,
				price INTEGER,
                brand VARCHAR(50),
				avg_rating FLOAT,
                total_ratings INTEGER,
				url TEXT,
				image_url TEXT,
				created_on TIMESTAMP,
                tiki_now BOOLEAN
			);
		"""
        cur.execute(query)

### Further work
### We currently have `Categories` and `Products` tables; `Users` and `Comments` tables belong to further work.
```python 
query = """
			CREATE TABLE IF NOT EXISTS users (
				user_id SERIAL NOT NULL PRIMARY KEY,
				username VARCHAR(50),
				created_on TIMESTAMP
			);
		"""
        cur.execute(query)

        query = """
			CREATE TABLE IF NOT EXISTS comments (
				user_id INTEGER NOT NULL,
				product_id INTEGER NOT NULL,
				PRIMARY KEY (user_id, product_id),
				content TEXT,
				rating FLOAT,
				CONSTRAINT comments_user_id_fkey FOREIGN KEY (user_id) REFERENCES users(user_id),
				CONSTRAINT comments_product_id_fkey FOREIGN KEY (product_id) REFERENCES products(product_id)
			);
		   """
        cur.execute(query)

        connection.commit()

    except Exception as error:
        print('ERROR: Create table fail -', error)
        connection.rollback()

    finally:
        cur.close()
        connection.close()

```
### 4. Insert data to PostgresSQL Database
```python
def insert_row(data, table_name, default=True):
    """Insert data into table_name
    """
    # print("INFO: Insert data into {}".format(table_name))

    try:
        connection = db_get_connection()
        if not connection:
            print('ERROR: Connection db fail')
            return
        cur = connection.cursor()

        cols = 'DEFAULT,' if default else ''
        for i in range(len(data)-1):
            cols = cols + '%s,'
        cols = cols + '%s'

        query = 'INSERT INTO ' + table_name + ' VALUES (' + cols + ');'
        cur.execute(query, data)

        connection.commit()

    except Exception as error:
        print('ERROR: Insert into DB fail -', error)
        connection.rollback()

    finally:
        cur.close()
        connection.close()
```
```python 
def execute_query(query):
    """Execute an SELECT query
    """
    print("INFO: Execute {}".format(query))

    try:
        connection = db_get_connection()
        if not connection:
            print('ERROR: Connection db fail')
            return
        cur = connection.cursor()

        cur.execute(query)

        connection.commit()

    except Exception as error:
        print('ERROR: Insert into DB fail -', error)
        connection.rollback()
        return []

    finally:
        return cur.fetchall()
        cur.close()
        connection.close()
```
## III. Data analysis on Jupyter notebook
We analyzed the data by pushing them into **Panda dataframe** and using these packages
```python
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
%matplotlib inline
import warnings
warnings.filterwarnings('ignore')
```
**We answered these questions:**

* How many products in Tiki, belongs to how many categories?
* How many duplicated products?
* How many products are there in each category?
* How many sellers are there on Tiki?
* How many brands in Tiki? Name top 10 brands with the most products?
* How many products have tikiNow service?
* What is the rating average of all products?
* Which category has the highest rating?
* Which category has the biggest number of comments?
* Which product has the biggest number of comments?
* What is the hottest category or product by review?
* What is the worst category or product by review?
* What are the cheapest and the most expensive products?
* What are the mean prices of each category?
* How customers comment on those products?
* What kinds of books does Tiki offer?
* The relationship between books price and customer's satisfactory?

## IV. Push and pull the solution to a new Github repo
[GitHub](https://github.com/) offers user accounts for individuals and organizations for teams of people working together.

#### Sign up for a Github account
If you have not had Github account. Create one by following steps

1. Go to GitHub's Pricing page.
2. Read the information about the different products and subscriptions that GitHub offers, then click the upgrade button under the subscription you'd like to choose.
3. Follow the prompts to create your personal account or organization.

#### Creating a new repository
1. In the upper-right corner of any page, click `+` and then click `New repository`.

![](https://i.imgur.com/GINP04I.png)

2. Type a short, memorable name for your repository. For example, `hello-world`.
    
![](https://i.imgur.com/KmvlTTJ.png)


3. Optionally, add a description of your repository. For example, `My first repository on GitHub`.
    ![](https://i.imgur.com/mxjiGSU.png)


4. Choose to make the repository either public or private. Public repositories are visible to the public, while private repositories are only accessible to you, and people you share them with. For more information, see ["Setting repository visibility."](https://help.github.com/en/articles/setting-repository-visibility)
    ![](https://i.imgur.com/oetL1sy.png)


5. Select Initialize this repository with a README.
    ![](https://i.imgur.com/ChNNjuy.png)


6. Click `Create repository`.

Congratulations! You've successfully created your first repository, and initialized it with a README file.

#### Pulling down a remote repository:


1.  On GitHub, navigate to the main page of the repository.
2.  Under the repository name, click `Clone or download`.

     ![](https://i.imgur.com/T2XybjO.png)

3. In the Clone with HTTPs section, click the arrow to copy the clone URL for the repository.
   
   ![](https://i.imgur.com/8bEDb2K.png)


4.  Open Git Bash. Change the current working directory to the location where you want the cloned directory to be made. Type `git clone`, and then paste the URL you copied in Step 3.

    `$ git clone https://github.com/YOUR-USERNAME/YOUR-REPOSITORY`

    Press `Enter`. Your local clone will be created.

    
    > Cloning into `Spoon-Knife`...
    > remote: Counting objects: 10, done.
    > remote: Compressing objects: 100% (8/8), done.
    > remove: Total 10 (delta 1), reused 10 (delta 1)
    > Unpacking objects: 100% (10/10), done.`

## Further work
We currently have `Categories` and `Products` tables; `Users` and `Comments` tables belong to further work.
