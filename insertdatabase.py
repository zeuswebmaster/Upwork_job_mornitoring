import mysql.connector
import datetime

class InsertDB:
    
    mydb_name = "upworkjob_db"

    def insert_document(self, documents):
        print(documents)

        # ************** DIGITAL SERVER ***************#
        mydb = mysql.connector.connect(
            user = "root",
            password = "",
            host = "localhost"
        )

        mycursor = mydb.cursor()

        mycursor.execute("CREATE DATABASE IF NOT EXISTS " + self.mydb_name + " CHARACTER SET utf8 COLLATE utf8_general_ci")

        # ********** DIGITAL OCEAN SERVER ***********#
        mydb = mysql.connector.connect(
            user = "root",
            password = "",
            host = "localhost",
            database = self.mydb_name
        )

        documents = documents[0]
        print(documents)

        mycursor = mydb.cursor()

        stmt = "SHOW TABLES LIKE 'upwork_job'"
        mycursor.execute(stmt)
        result = mycursor.fetchone()

        if not result:
            sql = "CREATE TABLE upwork_job (id INT(11) UNSIGNED AUTO_INCREMENT PRIMARY KEY, JobTitle VARCHAR(100), JobUrl VARCHAR(50), Budget VARCHAR(15), Duration VARCHAR(20), WeeklyHours VARCHAR(30), PaymentVerification VARCHAR(10), ClientCountry VARCHAR(20), TotalSpent VARCHAR(20), FeedBack VARCHAR(50), FreelancerLocation VARCHAR(200), FreelancerLevel VARCHAR(20), JobPostedAccounts VARCHAR(20), JobHiredNumbers VARCHAR(20), AvgHourlyRate VARCHAR(30), MemberSince VARCHAR(30), CreatedTime VARCHAR(20), INDEX (JobUrl))"

            mycursor.execute(sql)
            mydb.commit()


        sql = "SELECT JobUrl FROM upwork_job WHERE JobUrl='{}'".format(documents[1])
        mycursor.execute(sql)
        job_result = mycursor.fetchone()

        if not job_result:
            insert_sql = """INSERT INTO upwork_job (JobTitle, JobUrl, Budget, Duration, WeeklyHours, PaymentVerification, ClientCountry, TotalSpent, FeedBack, FreelancerLocation, FreelancerLevel, JobPostedAccounts, JobHiredNumbers, AvgHourlyRate, MemberSince, CreatedTime) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

            mycursor.execute(insert_sql, documents)
            mydb.commit()

        # else:
        #     update_sql = 'UPDATE upwork_job SET JobTitle="{0}", Budget="{1}", Duration="{2}", PermitIssuedDate="{3}", WeeklyHours="{4}", ProjectAddress="{5}", ProjectCity="{6}", ProjectState="{7}", PermitLink="{8}", ApplicantName="{9}", ContractorName="{10}", Tags="{11}", UpdatedTime="{12}" WHERE PermitNo="{13}"'.format(documents[1], documents[2], documents[3], documents[4], documents[5], documents[6], documents[7], documents[8], documents[10], documents[11], documents[12], documents[13], datetime.datetime.now(), documents[0])
        #     print(update_sql)
        #     mycursor.execute(update_sql)
            
        #     mydb.commit()
        print("==================> Now time:", datetime.datetime.now())