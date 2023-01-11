import psycopg2

conn = psycopg2.connect(database="provider_directory",
                        user='postgres', 
                        password='',
                        host='localhost',
                        port='5432')
cursor = conn.cursor()

update_providers_table_file = open('update_providers_table.sql', 'r')
cursor.execute(update_providers_table_file.read())
conn.commit()
conn.close()