import csv,requests
import pymysql

response = requests.get("https://api.statistiken.bundesbank.de/rest/download/BBK01/SUD231?format=csv&lang=en")
# print(response.headers['content-type'])
# print(response.__dict__['_content'])
data=response.__dict__['_content']


#Writing the api data into csv
with open('testfile.csv', "wb") as f: 
    f.write(data)

data_read = []
with open("testfile.csv") as fp:
    reader = csv.reader(fp)
    data_read = [row for row in reader]
    print(data_read)
    data_read = data_read[8:-1]


connection = pymysql.connect(host='localhost',
                             user='root',
                             password='password',
                             database='db',
                             cursorclass=pymysql.cursors.DictCursor)

#cursor = connection.cursor()

with connection:
    connection.cursor().execute("truncate table db.users")
    with connection.cursor() as cursor:
        for row in data_read:
            year_data,month_data=row[0].split("-")
            sql = "INSERT INTO users (years, month, volume ) VALUES (%s, %s, %s)"
            cursor.execute(sql, (year_data, month_data, row[1]))
    connection.commit()

    with connection.cursor() as cursor:
        sql = "SELECT * from users"
        cursor.execute(sql)
        result = cursor.fetchone()
        print(result)
