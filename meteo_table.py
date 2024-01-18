import psycopg2

try:
    # Connect to your postgres DB
    conn = psycopg2.connect(
        host="localhost",  # or the IP address where your DB is hosted
        dbname="HARPA",  # your database name
        user="user",  # your username
        password="password",  # your password
        port="5432"  # the port number, default is 5432 for PostgreSQL
    )

    # Open a cursor to perform database operations
    cur = conn.cursor()

    # Execute a query
    cur.execute("SELECT * FROM HARPA.aggregazione_fascia_oraria")

    # Retrieve query results
    records = cur.fetchall()

    # Close the cursor and connection
    cur.close()
    conn.close()

    # Print results
    for record in records:
        print(record)

except psycopg2.Error as e:
    print(f"Unable to connect to the database: {e}")
