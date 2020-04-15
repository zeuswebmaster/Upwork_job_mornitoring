import json
import requests
from datetime import datetime
from datetime import timedelta
from bs4 import BeautifulSoup
import time, threading, re, signal
from selenium.webdriver import Firefox
from seleniumwire import webdriver as wd

from insertdatabase import InsertDB

######---------Search Option----------#####
# level = ["Entry", "Intermediate", "Expert"]                     ###   Freelancer Level
level_array = ["Intermediate", "Expert"]
average_rate = 25                                             ###   Clent spent averate hourly rate
fixed_amount = 5000                                             ###   Fixed Budget
weekly_hours_array = ["Less than 10", "10-30", "30+"]                 ###   Weekly hours
# weekly_hours_array = ["Less than 10", "30+"] 
period_array = ["1 month", "1-3 months", "3-6 months", "6 months+"]   ###   Project Duration
# client_country_array = ["United Sates", "Others"]                 ###   Client Country
# freelancer_country = ["Europe", "America", "Ukraine"]           ###   Freelancer Country
# client_feedback = 4                                             ###   Received Client's feedback
# total_spent = 1000                                              ###   Client Total Spent

all_loops = 0
first_title = ""
title_val = ""
refresh_status = 0


class ProgramKilled(Exception):
    pass

def signal_handler(signum, frame):
    raise ProgramKilled

class Job(threading.Thread):
    def __init__(self, interval, execute):
        threading.Thread.__init__(self)
        self.daemon = False
        self.stopped = threading.Event()
        self.interval = interval
        self.execute = execute
        # self.args = args
        # self.kwargs = kwargs
        
    def stop(self):
        self.stopped.set()
        self.join()
                
    def run(self):
        while not self.stopped.wait(self.interval.total_seconds()):
            self.execute()

def query_jobs(d): 
    global all_loops
    """
    Query the jobs
    Return the js object or None on error
    """
    del d.requests           

    try:
        d.get('https://www.upwork.com/ab/find-work/')
        time.sleep(10)
        myfeedBtn = d.find_element_by_id("my-job-feed-topic-link")
        print(myfeedBtn)

        myfeedBtn.click()
        time.sleep(10)
        
        # print(d.requests)
        all_loops = all_loops + 1
        for r in d.requests:
            # if 'find-work/api/feeds/search?user_location_match=1' in r.path:
            if 'https://www.upwork.com/ab/find-work/api/feeds/search' == r.path:
                print(r.path, r.response.status_code, all_loops)
                return json.loads(r.response.body)
    except Exception as e:
        print ("Error in query_jobs :", e)

    return None


def query_jobs_second(d, resultSetTs, moreBtn_click_accounts):
    
    del d.requests
    # moreBtn_click_accounts = 0
    try:
        load_moreBtn = d.find_element_by_id("load-more-button")
        print("Load More Jobs Button Xpath----> : ", load_moreBtn)
        load_moreBtn.click()
        time.sleep(5)

        pages = 10 * moreBtn_click_accounts

        query_url = "https://www.upwork.com/ab/find-work/api/feeds/search?max_result_set_ts={0}&paging={1};10".format(resultSetTs, pages)
        # https://www.upwork.com/ab/find-work/api/feeds/search?max_result_set_ts=1586716215642&paging=10;10
        print(query_url)
        
        for r in d.requests:
            if query_url == r.path:
                print(r.path, r.response.status_code, "more jobs option")
                return json.loads(r.response.body)
    except Exception as e:
        print("Error in query_jobs_second : ", e)
    
    return None



def monitor_jobs_crawler(d, d1):

    global title_val, all_loops, refresh_status
    nowtime = datetime.now()
    nowtimeUTC = datetime.utcnow()

    nowtimestr = nowtime.strftime("%Y-%m-%d %H:%M:%S")
    nowtimeUTCstr = nowtimeUTC.strftime("%Y-%m-%d %H:%M:%S")

    print("NowTime---------------   -------> : ", nowtimestr)
    print("NowUTCTime-------------------> : ", nowtimeUTCstr)
    
    counts = 1
    moreBtn_click_accounts = 1
    
    result = query_jobs(d)
    if result is None:
        print('Error. Result is None')
        return 

    resultSetTs = result["paging"]["resultSetTs"]
    print("Next Paging ID--------------------->", resultSetTs)

    while True:
        
        if refresh_status != 0 and counts % 10 == 1:
            result = query_jobs_second(d, resultSetTs, moreBtn_click_accounts)
            if result is None:
                print("Error, Result is None")
                return
            moreBtn_click_accounts = moreBtn_click_accounts + 1

        for job in result["results"]:


            print("------------------------------------------------------------->", counts)
            
            if counts >=50:
                return

            nowtime = datetime.now()
            title           = job["title"]
        
            if counts == 1:
                if title_val == title:
                    return
                first_title = title
                if all_loops == 1:
                    title_val = first_title
                    print("Store the first project Title: ", title_val)

            # if all_loops == 1:
                
            #     if counts == 0: 
            #         title_val = title
            #         print("Store the first project Title: ", title_val)
            
            if counts != 1:
                if title_val == title:
                    print(title_val, title, "---------------------> title is the same")
                    title_val = first_title
                    
                    return


            print("First Job title------------------> ", first_title)
            print("Val Job title--------------------> ", title_val)

            createdOn       = job["createdOn"]
            type            = job["type"]
            ciphertext      = job["ciphertext"]
            # description     = job["description"]
            # category2       = job["category2"]
            # subcategory2    = job["subcategory2"]
            # skills          = job["skills"]
            duration        = job["duration"]
            shortDuration   = job["shortDuration"]
            engagement      = job["engagement"]
            shortEngagement = job["shortEngagement"]
            amount          = int(job["amount"]["amount"])
            recno           = job["recno"]
            uid             = job["uid"]
            client_paymentverification = job["client"]["paymentVerificationStatus"]
            cient_country   = job["client"]["location"]["country"]
            totalSpent      = job["client"]["totalSpent"]
            totalReviews    = job["client"]["totalReviews"]
            totalFeedback   = job["client"]["totalFeedback"]
            lastContractPlatform = job["client"]["lastContractPlatform"]
            lastContractRid   = job["client"]["lastContractRid"]
            lastContractTitle = job["client"]["lastContractTitle"]
            feedbackText      = job["client"]["feedbackText"]
            try:
                feedbackText = (feedbackText.split(","))[0]
            except:
                feedbackText = ""
            companyOrgUid     = job["client"]["companyOrgUid"]

            freelancersToHire = job["freelancersToHire"]
            enterpriseJob     = job["enterpriseJob"]
            tierText          = job["tierText"]
            tier              = job["tier"]
            tierLabel         = job["tierLabel"]
            propoaslTier      = job["proposalsTier"]

            prefFreelancerLocation = job["prefFreelancerLocation"]
            if len(prefFreelancerLocation) == 0:
                prefFreelancerLocationText = ""
            else:
                for freelancerLocation in prefFreelancerLocation:
                    prefFreelancerLocationText = freelancerLocation + ", "

                prefFreelancerLocationText = prefFreelancerLocationText[:-2]

            publishedOn = job["publishedOn"]
            skills_1      = job["attrs"]
            skillStr      = ""

            for skill in skills_1:
                skillStr = skillStr + skill["prettyName"] + ", "
            limit = len(skillStr) - 2
            skillStr = skillStr[:limit]

            isLocal          = job["isLocal"]
            locations        = job["locations"]
            hourlyBudgetText = job["hourlyBudgetText"]


            del d1.requests
            second_query_url = "https://www.upwork.com/jobs/{}".format(ciphertext)
            d1.get(second_query_url)
            time.sleep(10)
            #second_response = requests.get(second_query_url, headers=headers, timeout=10)
            #src = second_response.text
            src = d1.page_source
            soup = BeautifulSoup(src, 'lxml')

            #print("StatusCode-------------------->", response.status_code)


            job_posting_status = soup.findAll("li", {"data-qa": "client-job-posting-stats"})

            job_posted_accounts = 0
            job_hire_rate = 0
            if job_posting_status:
                job_posted_accounts = job_posting_status[0].find("strong", {"class": "primary"}).getText()
                job_posted_accounts = ''.join(job_posted_accounts).strip() if job_posted_accounts else ""
                job_posted_accounts = int((job_posted_accounts.split(" "))[0])
            
                job_hire_rate = job_posting_status[0].find("div", {"class": "text-muted"}).getText()
                job_hire_rate = ''.join(job_hire_rate).strip() if job_hire_rate else ""
                job_hire_rate = int((((job_hire_rate.split(","))[0]).split(" "))[0].replace("%", ""))
            
            try:
                avg_hourly_rate = soup.findAll("strong", {"data-qa": "client-hourly-rate"})[0].getText()
                avg_hourly_rate = ''.join(avg_hourly_rate).strip() if avg_hourly_rate else ""
                avg_hourly_rate = float(((avg_hourly_rate.split(" "))[0].replace("/hr", "")).replace("$", ""))
            except:
                avg_hourly_rate = str("New Client")
            

            try:
                hire_accounts = soup.findAll("div", {"data-qa": "client-hires"})[0].getText()
                
                hire_accounts = ''.join(hire_accounts).strip() if hire_accounts else ""
                print("Hire_Accounts---------------------!!!!!!--> : ", hire_accounts)
                hire_accounts = hire_accounts.split("\n")[0]
                print("Hire_Accounts---------------------!!!!!!--> : ", hire_accounts)
            except:
                hire_accounts = 0


            try:
                client_spent_hours = soup.findAll("div", {"data-qa": "client-hours"})[0].getText()
                client_spent_hours = ''.join(client_spent_hours).strip() if client_spent_hours else ""
                client_spent_hours = int((client_spent_hours.split(" "))[0])
            except:
                client_spent_hours = 0
            
            try:
                client_created_date = soup.findAll("li", {"data-qa": "client-contract-date"})[0].find("small", {"class" : "text-muted"}).getText()
                client_created_date = ''.join(client_created_date).strip() if client_created_date else ""
                client_created_date = client_created_date.replace("Member since ", "")
            except:
                client_created_date = "Private job"


            print("Title--------------------> :", title)
            print("Job URL------------------> :", second_query_url)
            # print("ProjectCreated Date------> :", createdOn)
            # print("Job Type-----------------> :", type)
            # print("JobDetailsUrlid----------> :", ciphertext)
            # print("Description--------------> :", description)
            # print("Category2----------------> :", category2)
            # print("SubCategory2-------------> :", subcategory2)
            # print("Skills-------------------> :", skills)
            print("Duration-----------------> :", duration)
            print("Engagement---------------> :", engagement)
            print("ShortDuration------------> :", shortDuration)
            print("ShortEngagement----------> :", shortEngagement)
            print("Amount-------------------> :", amount)
            # print("RecNo--------------------> :", recno)
            # print("Uid----------------------> :", uid)
            print("ClientPaymentVerifcation-> :", client_paymentverification)
            print("ClientCountry------------> :", cient_country)
            print("TotalSpent---------------> :", totalSpent)
            print("TotalReviews-------------> :", totalReviews)
            print("TotalFeedback------------> :", totalFeedback)
            # print("LastContractPlatform-----> :", lastContractPlatform)
            # print("LastContractRid----------> :", lastContractRid)
            # print("lastContractTitle--------> :", lastContractTitle)
            print("FeedbackText-------------> :", feedbackText)
            # print("CompanyOrgUid------------> :", companyOrgUid)
            # print("FreelancersToHire--------> :", freelancersToHire)
            # print("EnterpriseJob------------> :", enterpriseJob)
            print("TierText-----------------> :", tierText)
            # print("Tier---------------------> :", tier)
            # print("Tierlabel----------------> :", tierLabel)
            # print("PropoaslTier-------------> :", propoaslTier)
            print("PrefFreelancerLocation---> :", prefFreelancerLocationText)

            # print("PublishedOn--------------> :", publishedOn)
            # print("SkillStr-----------------> :", skillStr)
            # print("IsLocal------------------> :", isLocal)
            # print("Locations----------------> :", locations)
            # print("HourlyBudgetText---------> :", hourlyBudgetText)

            print("Job Posted Accounts------> :" , job_posted_accounts)
            print("Job Hired Accounts-------> :" , hire_accounts)
            print("Job Hire Rate------------> :" , job_hire_rate)
            print("Average Hourly Rate------> :" , avg_hourly_rate)
            print("Member Since-------------> :" , client_created_date)

            insertdb = InsertDB()
            create_time = str(nowtime)  
            create_time_time = (create_time.split(" "))[1]
            create_time_time = create_time_time[0:4]
            data_base = []
            
            level_status = False
            weekly_hours_status = False
            period_status = False

            for level in level_array:
                if level in tierText:
                    level_status = True
            try:
                for weekly_hour in weekly_hours_array:
                    print(weekly_hour, shortEngagement)
                    if weekly_hour in shortEngagement:
                        weekly_hours_status = True
                        print("weekly hours----------------------> True!!!!")
                        break
            except:
                weekly_hours_status = False
            
            try:
                for period in period_array:
                    if period in shortDuration:
                        period_status = True
            except:
                period_status = False

            print("Level Status------------------->", level_status)
            if avg_hourly_rate != "New Client":
                avg_hourly_rate_text = str(avg_hourly_rate) + "$/hr"
            else:
                avg_hourly_rate_text = " "
            job_hire_rate_text = str(job_hire_rate) + "% hire rate"
            
            if avg_hourly_rate != "New Client":
                print("This client isn't new person on upwork")
                if avg_hourly_rate >= average_rate:
                    print("평균 지불 hourly rate가 40 이상인 경우:")
                    if ("Intermediate" in tierText or "Expert" in tierText) :
                        print("intermediate, expert 레벨의 과제는 과제 기간에 상관없이 현시한다")
                        if amount == 0:
                                budget = "Hourly"
                        else:
                            budget = amount
                        data_base.append((title,
                                            second_query_url,
                                            budget,
                                            duration,
                                            shortEngagement,
                                            client_paymentverification,
                                            cient_country,
                                            totalSpent,
                                            feedbackText,
                                            prefFreelancerLocationText,
                                            tierText[0:6],
                                            job_posted_accounts,
                                            hire_accounts,
                                            avg_hourly_rate_text,
                                            client_created_date,
                                            create_time_time))
                        insertdb.insert_document(data_base)

                elif avg_hourly_rate < average_rate:
                    print("과거의 평균지불레이트 40미만이면 다음의 조건에 맞는 과제들만 현시한다.")
                    if (level_status and weekly_hours_status and period_status) or amount >= fixed_amount:
                        print("Expert, 10 or 30hours more, 1 month more or 10k more")
                        if amount == 0:
                                budget = "Hourly"
                        else:
                            budget = amount
                        data_base.append((title,
                                        second_query_url,
                                        budget,
                                        duration,
                                        shortEngagement,
                                        client_paymentverification,
                                        cient_country,
                                        totalSpent,
                                        feedbackText,
                                        prefFreelancerLocationText,
                                        tierText[0:6],
                                        job_posted_accounts,
                                        hire_accounts,
                                        avg_hourly_rate_text,
                                        client_created_date,
                                        create_time_time))
                        insertdb.insert_document(data_base)

            else:
                print(level_status, weekly_hours_status, period_status)
                if (level_status and weekly_hours_status and period_status) or amount >= fixed_amount:
                    print("Expert, 10 or 30hours more, 1 month more or 10k more")
                    if amount == 0:
                            budget = "Hourly"
                    else:
                        budget = amount
                    data_base.append((title,
                                    second_query_url,
                                    budget,
                                    duration,
                                    shortEngagement,
                                    client_paymentverification,
                                    cient_country,
                                    totalSpent,
                                    feedbackText,
                                    prefFreelancerLocationText,
                                    tierText[0:6],
                                    job_posted_accounts,
                                    hire_accounts,
                                    avg_hourly_rate_text,
                                    client_created_date,
                                    create_time_time))
                    insertdb.insert_document(data_base)

            
            
            counts += 1

        if all_loops == 1:
            break

        refresh_status = refresh_status + 1



if __name__ == '__main__':
    print("---------------------------------Upwork Crawler Start------------------------------------")
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    monitor_period = 200  #seconds

    path = "driver\\geckodriver.exe"
    
    d = wd.Firefox(executable_path=path)
    d1 = wd.Firefox(executable_path=path)
    d.get('https://www.upwork.com/')
    d1.get('https://www.upwork.com/')
    time.sleep(10)
    d.get('https://www.upwork.com/ab/account-security/login')
    d1.get('https://www.upwork.com/ab/account-security/login')
    input('Login to upwork in the browser and press enter:')


    while True:
        refresh_status = 0
        monitor_jobs_crawler(d, d1)
        time.sleep(monitor_period)
        