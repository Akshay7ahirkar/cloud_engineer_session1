import mysql.connector
abc=mysql.connector.connect(
    host='localhost',
    user='root',
    password='root'
)
print(abc)