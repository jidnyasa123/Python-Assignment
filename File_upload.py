import sys
import os
import json
import pandas as pd
import mysql.connector as c


def upload_file(source_dir,mysql_details,destination_table):
    dirs = os.listdir(source_dir)

    for file in dirs:
    

        emp = pd.read_csv(os.path.join(source_dir,file))
        print(emp)

        with open(mysql_details) as data_file:
            data1 = json.load(data_file)[0]
            mysql_ip = data1["mysql_ip"]
            port = data1["port"]
            username = data1["username"]
            password = data1["password"]
            database = data1["database"]
            print(f'{mysql_ip}-{port}-{username}-{password}')

            conn=c.connect(host=mysql_ip,user=username,passwd=password,database=database)
            if conn.is_connected():
                cursor = conn.cursor()

                print("Successfully Connected....")
                for i,row in emp.iterrows():
                        #here %S means string values 
                        sql = "INSERT INTO emp VALUES (%s,%s,%s,%s)"
                        

                        cursor.execute(sql, tuple(row))
                        print("Record inserted")
                        # the connection is not auto committed by default, so we must commit to save our changes
                        conn.commit()


            else:
                print("Some Connectivity Isuue...")


def main(argv):
    source_dir = argv[1]
    mysql_details = argv[2]
    destination_table = argv[3]
    upload_file(source_dir,mysql_details,destination_table)

if __name__ == "__main__":
    sys.exit(main(sys.argv))







