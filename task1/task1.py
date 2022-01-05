from bs4 import BeautifulSoup
import requests
import pandas as pd
import json
import time
from fake_useragent import UserAgent
import psycopg2
import s 
# function to store data in database
def database_conectivity(c_url,p_title,p_image,p_price,p_details):
    hostname='localhost'
    database='credicxo'
    username='postgres'
    pwd=s.password
    port_id=5432
    conn =None
    cur=None

    try:
        conn=psycopg2.connect(
            host=hostname,
            dbname=database,
            user=username,
            password=pwd,
            port=port_id)

        cur= conn.cursor()

        create_script=''' CREATE TABLE IF NOT EXISTS Task1(
                        url varchar(500),
                        p_title   varchar(500) ,
                        p_img_url varchar(500) ,
                        p_price varchar(500),
                        p_details varchar(500)

                        )'''
        cur.execute(create_script)
        
        insert_sctipt='INSERT INTO Task1(url,p_title,p_img_url,p_price,p_details) VALUES(%s,%s,%s,%s,%s)'
        insert_value=(c_url,p_title,p_image,p_price,p_details)
        cur.execute(insert_sctipt,insert_value)


        
        conn.commit()
        
    except Exception as error:
        print(error)
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()






#create header for your os and broser
# headers={'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36', }
ua = UserAgent()

#read csv file using pandas
data=pd.read_csv('task1.csv')
#get country code
country=data['country']
#get asin
asin=data['Asin']

l=[]
l1=[]
i=0
#start time of process
start=time.time()
for (c,a) in zip(country,asin):
        i+=1
        dict1={}
        #creating url 
        url=f"https://www.amazon.{c}/dp/{a}"
        # url="https://www.amazon.in/dp/B08LRDM44F?th=1"
        #sending request to the url
        r = requests.get(url,headers={'User-Agent':ua.random})
        #create object of bs4
        
        if(r.status_code)==404:
            res={'url':url+" Not Available"}
            l1.append(res)
            database_conectivity(url,"Not Available","404","404","404")
        elif(r.status_code)==200:
            #scrap title
            
            soup = BeautifulSoup(r.content, 'html5lib')
     
            try:           
                title = soup.find("span", attrs={"id":'productTitle'})
                
                title_value=title.string
                title_string=title_value.strip()
            except AttributeError:
                title_string="NA"

            # scrap price
            try:
                price=soup.find("span",attrs={'class':'a-offscreen'})
                price_value=price.string
                price_string=price_value.strip()
            except AttributeError:
                price_string="NA"

            # scrap img url
            try:
                p_img = soup.find(id="imgTagWrapperId")
                imgs_str = p_img.img.get('data-a-dynamic-image') 
                imgs_dict = json.loads(imgs_str)
                num_element = 0 
                link = list(imgs_dict.keys())[num_element]
            except AttributeError :
                 link="NA"   

            # scrap product details
            for ultag in soup.find_all('ul', {'class': 'a-unordered-list a-vertical a-spacing-mini'}):
                    for litag in ultag.find_all('li'):
                        l.append(litag.text)
        
            database_conectivity(url,title_string,price_string,link,l)
            dict1={'url':url,"product_title":title_string,"product_price":price_string,"product_img_url":link,"product_details":l}    
            # appending in format of key:value pairs
            l1.append(dict1)

        else:
            print(r.status_code)
        print(i)
        # for every 100 urls calculate time
        if i%100==0:
            end=time.time()
            print(end - start)
            start=time.time()
            
out_file = open("myfile.json", "w")
json.dump(l1, out_file, indent = 6)
out_file.close() 