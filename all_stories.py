
# coding: utf-8

# In[1]:


from bs4 import BeautifulSoup as bs
import urllib
import requests
import os
from datetime import datetime
from datetime import timedelta
import re

import logging
from time import sleep
import multiprocessing
import threading
import time
import os
from pyArango.connection import Connection

_db = Connection(arangoURL='http://35.198.104.86:8529', username='root', password='USFiXWWoLGD9d0iv')
news_db = _db['news']
_COLLECTION_content = news_db['content']
GLOBAL_BASE_URL = 'https://www.dawn.com/archive/'
GLOBAL_HEADERS = [('User-Agent',
                   'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1941.0 Safari/537.36')]


class SleepTimer:
    def __init__(self):
        self.fail = 0
        self.success = 0

    
    def updateFail(self):
        self.fail += 1
        

    def updateSuccess(self):
        self.success += 1
        
    def fail_rate(self):
        if float(self.success) < 1:
            return self.fail * 2
        
        return self.fail / float(self.success)
    
    def sleepTimeCalculator(self):
        print('FAILS: ', self.fail, 'SUCESS: ', self.success, 'RATIO: ', self.fail_rate() )
        if (self.fail_rate() < 1):
            return 0
        else:
            if (self.fail_rate() > 5):
                return 30 
            return int(self.fail_rate() ** 2)

__SLEEPER = SleepTimer()

def write_to_file(name, data):
    print(name)
    with open('DATA/' + name, 'w') as outfile:
        outfile.write(data)


def normalize_uri(uri):
    return uri.replace('/', '_', )


def R(uri, data=None, base_url=GLOBAL_BASE_URL, headers=GLOBAL_HEADERS):
    return requests.get(base_url + uri, data, timeout=10)

def Rurl(url, data=None, headers=GLOBAL_HEADERS):
    return requests.get(url, data, timeout=10)




def url_generator(n, date_now, m=0):
    start = m * n
    end   = start + n 
    print('will yield from start {} to end {}'.format(start, end))
    while end > start:

        dt = date_now - timedelta(days=start)
        uri = dt.strftime("%Y-%m-%d")
        yield uri
        start += 1


def handle_response(r):
    if (r == True):
        __SLEEPER.updateSuccess()
    else:
        __SLEEPER.updateFail()

def main(limit, date):
    print('Starting main!, BATCH SIZE: ', limit)

    p = multiprocessing.Pool(1)
    i = 0
    limit = limit 
    results = []
    while not q.empty() and limit > i:
        results.append(p.apply_async(run, (date, i, q), callback=handle_response))
        i += 1
    p.close()
    p.join()
    if not q.empty():
        print(10*'*', limit, 'PROCESSED WITH ERRORS ... WILL SLEEP AND TRY AGAIN -> ', date, limit, 10*'*')

        sleep(__SLEEPER.sleepTimeCalculator())
        main(q.qsize(), date)
    else:
        print(10*'*', limit, 'PROCESSED -> ', date, limit, 10*'*')


def get_page(uri, tries):
    _try = 0
    while _try < tries:
        page = Rurl(uri)  # retrun page
        html = bs(page.content, 'lxml')  # parse page content with with beautiful soup
        page_length = len(str(page.content))
        
       
        
        if (page_length < 1000):
            #print('Excetion: Content too short, ', (len(str(page.content)), 'Try: ', tries))
            _try += 1
            continue
        
        else:
            return {
                    'headline': html.find('h2', {'class': 'story__title'}).get_text(),
                    'body': html.find('div', {'class': 'story__content'}).get_text(),
                    'raw': str(page.content)
            }

    raise Exception('Content too short, 403 ')

def extract_links(html):
    ahrefs = html.findAll('a')
    document_links = set()

    for href in ahrefs:
        skipped = []
        try:
            if (re.match(r'^(https://www\.dawn\.com/news).*', href['href'])):
                document_links.add(href['href'])
        except Exception as e:

            skipped.append(e)
    print('Skipped links: ', len(skipped))

    return document_links


def run(date, i, q, tries=1):
    #print('RUN SAYS: date, i, q', date, i, q)
    if (q.empty()):
        return False

    try:
        uri = q.get()
        
        #print('RUNNING FOR: ', uri)
        html = get_page(uri, tries)
        
        col = _COLLECTION_content
        
        doc = col.createDocument({
            'date': date,
            'story': html,
            'url': uri
        })
        doc.save()



        return True
    except Exception as e:
        #print('Exception: putting back url ->> ', uri, ' <<- due to ', e)
        print('url missed! waiting for server', uri.split('/')[-1])
        q.put(uri)
        return False


# run()

        
if __name__ == "__main__":
    manager = multiprocessing.Manager()
    q = manager.Queue(maxsize=0)
    import sys
    start = int(sys.argv[1])
    end   = int(sys.argv[2])

    if start >= end:
        print('start is less then end')
        sys.exit()
    
    for i in range(start, end, 2):
        #print('helllo!')
        aql = 'FOR d in links sort d.date limit '+ str(i) +', '+ str(2) +' return d'
        r = news_db.AQLQuery(aql)
        #print([rd['date'] for rd in r])
        for date in r:
            l = date['stories']
            n = 5
            mini_batches_of_stories = [l[i:i + n] for i in range(0, len(l), n)]
            j = 0
            for batch in mini_batches_of_stories:
                mini_batch_start_time = time.time()
                for story in batch:
                    q.put(story)

                main(len(batch), date['date'])
                j += 1
                print('MINI BACTH COMPLETE ... {}/{} TIME: {}'.format(j,len(mini_batches_of_stories), (time.time() - mini_batch_start_time)))
                print('')
                
                #sleep(__SLEEPER.sleepTimeCalculator())
            print(50*'*')
            print(50*'*')
            print('BACTH COMPLETE ... DATE {}, Stories {} '.format(date['date'], len(date['stories'])))
            print(50*'*')
            print(50*'*')
            
            print('')
        print('Multiple BATCHES COMPLETE  ... i -> {}'.format(i))
    print('done!!!!!!!!!')
#     for i in range(0, 1000):
         
#         for url in list(url_generator(3, datetime.now(), i)):
#             q.put(url)

#         main()
#         print('BATCH NO # {} COMPLETE'.format(i))
#         sleep(2)
#     print('before main')
#     print('done!')


# In[45]:




