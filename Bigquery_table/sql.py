import sqlite3
conn = sqlite3.connect('my database.sqlite')
cursor = conn.cursor()

print("opened database successfully")


# cursor.execute('''Create table school6 (ID INT PRIMARY KEY NOT NULL,NAME TEXT NOT NULL,AGE INT NOT NULL,ADDRESS CHAR(50),MARKS INT);''')
# cursor.execute("INSERT INTO school6(ID,NAME,AGE,ADDRESS,MARKS) VALUES(1,'Rohan',14,'Delhi',200)")
# cursor.execute("INSERT INTO school6(ID,NAME,AGE,ADDRESS,MARKS) VALUES(2,'Allen',14,'Banglore',150)")
# cursor.execute("INSERT INTO school6(ID,NAME,AGE,ADDRESS,MARKS) VALUES(3,'Martha',15,'Hydrabad',200)")
# cursor.execute("INSERT INTO school6(ID,NAME,AGE,ADDRESS,MARKS) VALUES(4,'Palak',15,'Kolkata',650)")

# conn.execute("delete from school6 where id = 2")
# conn.commit()
# for row in cursor.execute("SELECT ID,NAME,Address,MARKS FROM SCHOOL6"):
#     print("ID= ", row[0])
#     print("NAME= ", row[1])
#     print("ADDRESS= ", row[2])
#     print("MARKS= ", row[2], "\n")
# conn.commit()

for row in cursor.execute("select name from sqlite_master where type = 'table';"):
    print(row)
