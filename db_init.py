import datetime
import psycopg2


def db_get_connection():
    """ Return a database connection or None if error occurs
    """
    try:
        connection = psycopg2.connect(user="duong",
                                      password="P@ssw0rd",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="tiki5")
        return connection

    except Exception as error:
        print(error)
        return None


def create_tables():
    """ Create tables categories, products, users, comments
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
