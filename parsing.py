import pymysql
from bs4 import BeautifulSoup

import time
import requests
import re

# conneting mysql database
db = pymysql.connect( host = 'localhost', user = 'root', passwd = '', db='sitedb')
cursor = db.cursor()


# define parsing settings
URL = 'https://www.yelp.com/search?find_desc=Vegan+Cafe&find_loc=San+Francisco%2C+CA&ns=1'
HEADERS = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:71.0) Gecko/20100101 Firefox/71.0',
            'accept':'*/*'}


# body of parsing logic
def get_full_adress(address):
    full_adress = ''
    if address:
        first_adress = address.find('p').get_text()
        sub_adress = address.find_next_sibling().get_text()
        full_adress = first_adress + sub_adress
    return full_adress


def get_content(html):
    soup = BeautifulSoup(html, 'html.parser')
    time.sleep(10)
    vegan_cafe = soup.find_all(class_ = re.compile("container__09f24__21w3G"))
    time.sleep(5)

    for item in vegan_cafe:
        defaultData = {
            'header': '',
            'phone': '',
            'website': '',
            'tags': [],
            'address': '',
            'city': '',
            'post_index': '',
            'latitude': None,
            'longitude': None,
            'rating': None,
            'rating_google': None
        }

        header = item.find('h4')
        address = item.find('address')
        tags = item.find_all('div', class_='priceCategory')
        if tags:
            defaultData.update({'tags':tags.find('span')})
        full_adress = get_full_adress(address)
        if header: # add header if exist
            defaultData.update({'header' : header.find('a').get_text()})
            if full_adress:  # add address if exist
                defaultData.update({'address': full_adress})
                phone = address.find_previous_sibling()
                if phone:  # add phone if exist
                    defaultData.update({'phone': phone.get_text()})

        # insert each restaurant card in mysql database
        sql = "INSERT INTO sitedb.restaurants(header, phone, website, tags, address, city, post_index, latitude, longitude, rating, rating_google) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        cursor.execute(sql, (defaultData.header, defaultData.phone, defaultData.website, defaultData.tags, defaultData.address, defaultData.city, defaultData.post_index, defaultData.latitude, defaultData.longitude, defaultData.rating, defaultData.rating_google))

    # close db connection
    cursor.close()
    db.commit()
    db.close()


# end body of parsing logic
def get_html(url, params=None):
    r = requests.get(url, headers=HEADERS, params=params)
    return r


def parse():
    html = get_html(URL)
    if html.status_code == 200:
        get_content(html.text)
    else:
        print('error')

parse()