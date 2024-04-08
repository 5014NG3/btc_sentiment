import requests
from bs4 import BeautifulSoup
import re
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from urllib.parse import parse_qs, urlparse

import sqlite3

from sentiment import sentiment

import csv


import numpy as np

import time

from binance import Client

import json

import datetime



class coindesk_Scraper: 

    def __init__(self): 

        self.createDB()
        self.creds = self.getCreds()
        self.sentiment = sentiment()

        self.bad_paths = ['/learn/','/video/','/tag/','/es/','/podcasts/','/research/','https://','http://']

        self.bclient = Client(self.creds[0], self.creds[1], tld = 'us')

    #link scraper ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def article_scraper(self):

        starting_page = int(self.getCrashPage())
        self.scrapeAllArticles(starting_page)




    def getPageHTML(self,url):
        response = requests.get(url)
        status_code = response.status_code

        if status_code == 200:

            return BeautifulSoup(response.text, 'html.parser')

        else:
            return "null"



    def getPageContent(self, url):

        articles = set()
        
        soup = self.getPageHTML(url)

        if soup != "null":

            links = soup.find_all('a')
            for link in links:

                href = link.get('href')

                if href is not None and link.text is not '':
                    href = href.lower()
                    if '/20' in href and not self.hasBadPath(href) :
                        articles.add((link.text,href))

            for article in articles:

                title =  article[0]

                path = article[1]

                date = self.getPathDate(path)

                title_score = self.sentiment.score_text(title)

                content = self.getArticleContent(path)
                content_score = self.sentiment.score_text(content)


                # #print(path,date,title,content,title_score['neg'],title_score['pos'], title_score['neu'], content_score['neg'], content_score['pos'], content_score['neu'])
                # print(title,title_score)
                # print(content,content_score)
                # print()






                self.insertArticle(path,date,title,content,title_score['neg'],title_score['pos'], title_score['neu'], content_score['neg'], content_score['pos'], content_score['neu'])



    def scrapeAllArticles(self,starting_page):


        host = 'https://www.coindesk.com/tag/bitcoin/'

        last_page = 542

        for i in range(starting_page,last_page+1):

            print(i, "/",last_page)

            self.updateCrashpage(i)


            self.getPageContent(host + str(i))



    #content_scraper~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~



    def neutralizeText(self,text):
        # Tokenize the article into individual words
        combined_text = " ".join(text)


        words = word_tokenize(combined_text)

        # Get the English stopwords
        stop_words = set(stopwords.words('english'))

        # Filter out neutral words
        filtered_words = [word for word in words if word.lower() not in stop_words]

        return filtered_words
    
    def extractArticleContent(self,article):
    
        sponsor = 'Sponsored Content'
        disclosure = 'DISCLOSURE'
        edit = 'Edited by '

        edit_idx = -1


        sponsor_idx = article.index(sponsor)
        #keep stuff after
        article = article[sponsor_idx + 1:]

        disclosure_idx = article.index(disclosure)
        #keep stuff before
        article = article[:disclosure_idx]

        for i in range(len(article) - 1, -1, -1):
            if edit in article[i]:

                edit_idx = i
                break

        #keep stuff before

        if edit_idx != -1:
            article = article[:edit_idx]


        article = [string for string in article if 'Follow @' not in string]


        return article


    def getArticleTextNew(self,html):

        content = html.find_all(lambda tag: (tag.name in ['h1', 'h2'] or (tag.name == 'p' and not (tag.find_parent("div", class_="description") or (tag.find_parent("div", class_="name") and tag.find_parent("div", class_="name") != tag.parent)))))

        build_article = []

        for text in content:
            build_article.append(text.text)


        return build_article



    def getPathDate(self,path):
        pattern = r"/(\d{4}/\d{2}/\d{2})/"
        match = re.search(pattern, path)
        if match:
            return match.group(1)
        else:
            return "null"
        

    def hasBadPath(self,path):
        return any(substring in path for substring in self.bad_paths)

    def extractLangHelperNew(self,html):
        script_tag = html.find('script', {'data-cookieconsent': 'ignore'})
        if script_tag:
            content_language_match = re.search(r'"content_language":"(\w+)"', str(script_tag))
            if content_language_match:
                content_language = content_language_match.group(1)
                return content_language
            else:
                return "lang not found"
        else:
            return "no script tag"
        
    def cleanArticleNew(self,html):
        return " ".join( self.extractArticleContent( (self.getArticleTextNew(html))))
    
    def getArticleContent(self,path):

        html = self.getPageHTML("https://www.coindesk.com/" + path)

        article_content = "null"

        if html != "null":

            lang = self.extractLangHelperNew(html)

            if lang == 'en':

                article_content = self.cleanArticleNew(html)
        
        return article_content

    
    #datebase ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def createDB(self):
        self.createMetaDB()
        self.createArticlesDB()


    def createBitcoinDB(self):

        # Connect to the database (creates a new one if it doesn't exist)
        conn = sqlite3.connect('coindesk_btc.db')

        # Create a cursor object to execute SQL commands
        cursor = conn.cursor()

        # Create a table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS binance_btc (
                date TEXT PRIMARY KEY,
                open_price REAL,
                high_price REAL,
                low_price REAL,
                close_price REAL,
                volume REAL

            )
        ''')

        # Commit the changes and close the connection
        conn.commit()
        conn.close()

    def createMetaDB(self):
        conn = sqlite3.connect('coindesk_btc.db')

        # Create a cursor object to execute SQL commands
        cursor = conn.cursor()

        # Create a table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS metadata (
                last_page INTEGER 
            )
        ''')

        cursor.execute("INSERT INTO metadata (last_page) VALUES (?)", (1,))


        # Commit the changes and close the connection
        conn.commit()
        conn.close() 

    def getCrashPage(self):
        conn = sqlite3.connect("coindesk_btc.db")
        cursor = conn.cursor()

        # Retrieve the value from the "metadata" table
        cursor.execute("SELECT * FROM metadata")
        result = cursor.fetchone()

        # Close the connection
        conn.close()

        # Check if the result is None or the first element is None
        if result is None or result[0] is None:
            return -1

        return result[0]
    



    def updateCrashpage(self, new_value):
        conn = sqlite3.connect('coindesk_btc.db')
        cursor = conn.cursor()

        # Update a value in the table
        cursor.execute('''
            UPDATE metadata
            SET last_page = ?
        ''', (new_value,))  # Pass the new value as a parameter

        # Commit the changes and close the connection
        conn.commit()
        conn.close()

    def createArticlesDB(self):
        # Connect to the database (creates a new one if it doesn't exist)
        conn = sqlite3.connect('coindesk_btc.db')

        # Create a cursor object to execute SQL commands
        cursor = conn.cursor()

        # Create a table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS articles (
                url TEXT PRIMARY KEY,
                date TEXT,
                title TEXT,
                content TEXT,
                t_neg REAL,
                t_pos REAL,
                t_neu REAL,
                c_neg REAL,
                c_pos REAL,
                c_neu REAL

            )
        ''')

        # Commit the changes and close the connection
        conn.commit()
        conn.close()


    def insertArticle(self,url, date, title, content, t_neg, t_pos, t_neu,c_neg,c_pos,c_neu):
        # create a connection to the database
        conn = sqlite3.connect('coindesk_btc.db')

        # create a cursor object to execute SQL commands
        c = conn.cursor()

        try:
            c.execute("INSERT INTO articles (url, date, title, content, t_neg, t_pos, t_neu,c_neg,c_pos,c_neu) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (url, date, title, content, t_neg, t_pos, t_neu,c_neg,c_pos,c_neu))
            conn.commit()
        except sqlite3.IntegrityError:
            pass

        # save the changes and close the connection
        conn.close()


    # new_content_scraper
    def getRecentArticles(self):
        # Connect to the SQLite database
        conn = sqlite3.connect('coindesk_btc.db')

        # Create a cursor object
        cursor = conn.cursor()

        # Query the table and sort by the date column in ascending order
        query = '''
            SELECT date,url
            FROM articles
            ORDER BY date DESC
            LIMIT 20
        '''

        # Execute the query
        cursor.execute(query)

        # Fetch all the rows
        rows = cursor.fetchall()

        topten = []

        # Process the rows as needed
        for row in rows:
            topten.append(row)

        # Close the connection
        conn.close()

        return topten



    #bitcoin price data scraping
    def insertKline(self,date, open_price, high_price, low_price, close_price, volume):
        
        # create a connection to the database
        conn = sqlite3.connect('coindesk_btc.db')

        # create a cursor object to execute SQL commands
        c = conn.cursor()

        try:
            c.execute("INSERT INTO binance_btc (date, open_price, high_price, low_price, close_price, volume) VALUES (?, ?, ?, ?, ?, ?)", (date, open_price, high_price, low_price, close_price, volume))
            conn.commit()
        except sqlite3.IntegrityError:
            pass

        # save the changes and close the connection
        conn.close()

    def getCreds(self):
        with open('creds.json', 'r') as infile:
            data = json.load(infile)

        return (data['api_key'],data['api_secret'])
    

    def btcPriceScraper(self):
        symbol = 'BTCUSDT'
        klines = self.bclient.get_historical_klines(symbol, Client.KLINE_INTERVAL_1DAY, '1 Aug, 2021', '4 Jun, 2023')
        for kline in klines:
            ts = kline[0] / 1000  # Convert the timestamp to a Unix timestamp
            date_obj = datetime.datetime.utcfromtimestamp(ts)
            date = date_obj.date()

            open_price = float(kline[1])
            high_price = float(kline[2])
            low_price = float(kline[3])
            close_price = float(kline[4])
            volume = float(kline[5])

            






            print(date,open_price,high_price,low_price,close_price,volume)






        







cd_scraper = coindesk_Scraper()


cd_scraper.btcPriceScraper()

ra = cd_scraper.getRecentArticles()

#last day scraped
print(ra[0][0])
print(ra)

