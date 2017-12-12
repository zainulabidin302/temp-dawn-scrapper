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

from DB import DB

def getfiles(path):
    return [file for file in os.listdir(path) if os.path.isfile(os.path.join(path, file))]


GLOBAL_BASE_URL = 'https://www.dawn.com/archive/'
GLOBAL_HEADERS = [('User-Agent',
                   'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1941.0 Safari/537.36')]


db = DB(username='root', password='USFiXWWoLGD9d0iv', selected_db='news')

def write_to_file(name, data):
    print(name)
    with open('DATA/' + name, 'w') as outfile:
        outfile.write(data)


def normalize_uri(uri):
    return uri.replace('/', '_', )


def R(uri, data=None, base_url=GLOBAL_BASE_URL, headers=GLOBAL_HEADERS):
    return requests.get(base_url + uri, data, timeout=2)


existing_files = getfiles('./DATA')


def removeDoc(key):
    aql = "Remove @id in `skiped_links`"
    try:
        db.gcdb().AQLQuery(aql, bindVars={'id': key})
        return True
    except:
        return False


        # System modules


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
    #print('response is ', r)
    pass

def main():
    print('Starting main!')

    p = multiprocessing.Pool(3)
    i = 0
    limit = 6 
    results = []
    while not q.empty() and limit > i:
        results.append(p.apply_async(run, (i, q,), callback=handle_response))
        i += 1
    p.close()
    p.join()
    print('Good BYE!')
    if not q.empty():

        sleep(5)

        main()
    else:
        print('queue is empty')


def get_page(uri, tries):
    _try = 0
    while _try < tries:
        page = R(uri)  # retrun page
        html = bs(page.content, 'lxml')  # parse page content with with beautiful soup
        page_length = len(str(page.content))

        if (page_length < 1000):
            #print('Excetion: Content too short, ', (len(str(page.content)), 'Try: ', tries))
            _try += 1
            continue
        else:
            return html
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


def run(i, q, tries=1):
    if (q.empty()):
        return False

    try:
        uri = q.get()
        #print('RUNNING FOR: ', uri)
        html = get_page(uri, tries)
        links = extract_links(html)

        #print('Length -> ', len(links))

        col = db.findOrCreateCollection('links')
         
        doc = col.createDocument({
            'date': uri,
            'stories': list(links)
        })
        doc.save()



        return True
    except Exception as e:
        print('Exception: putting back url ->> ', uri, ' <<- due to ', e)
        q.put(uri)
        return False


# run()
if __name__ == "__main__":
    manager = multiprocessing.Manager()
    q = manager.Queue(maxsize=0)

    for i in range(0, 1000):
         
        for url in list(url_generator(3, datetime.now(), i)):
            q.put(url)

        main()
        print('BATCH NO # {} COMPLETE'.format(i))
        sleep(2)
    print('before main')
    print('done!')
