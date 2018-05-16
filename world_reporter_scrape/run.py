from common.browser import SelfClosingBrowser

from bs4 import BeautifulSoup
import boto3
import io
import pandas as pd
import pymysql
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def get_abstract(b):
    '''
    Fetches the abstract of a project when a single project URL
    is opened.
    '''
    abstract_xpath = ('//div[@class="home ng-scope"]'
                      '//p[@class="ng-scope ng-binding"]')
    # wait for page to expand abstract element
    abstract_p = WebDriverWait(b, 15).until(
        EC.presence_of_element_located((By.XPATH, abstract_xpath)))
    # random loading behaviour means better to take snapshot of html
    html = b.execute_script(
            "return document.getElementsByTagName('html')[0].innerHTML")
    soup = BeautifulSoup(html, 'html.parser')
    abstract = soup.find_all(
            'p',
            {'class': 'ng-scope ng-binding'}
            )[0].contents[0]
    return abstract.encode("utf-8")


def execute(**params):
    print("Getting input data")
    s3 = boto3.resource('s3')
    obj = s3.Object("nesta-inputs", params['url'])
    obj_io = obj.get()['Body'].read()
    obj.delete()

    #output = []
    print("Starting browser")
    with SelfClosingBrowser("/usr/bin/phantomjs", headless=True) as b:
        #for line in io.BytesIO(obj_io).read():
        for line in obj_io.decode("utf-8").split("\n"):
            print("Got",line)
            try:
                id_, url = line.split(",")
            except ValueError:
                break
            print("Got", id_, url)
            b.get(url)
            is_scraped = 0
            abstract = None
            exception = None
            try:
                # Get the data
                abstract = get_abstract(b)
            except Exception as err:
                # Save any error messages for debugging
                exception = str(err)            
            else:
                is_scraped = 1
            #output.append([abstract, exception, is_scraped, id_])

            # Write to database
            connection = pymysql.connect(read_default_file="innovation-mapping-tier0.config")
            #print("Writing",len(output),"results")
            with connection.cursor() as cur:
                sql = ("UPDATE `world_reporter_grants` "
                       "SET `abstract`=%s, `err_msg`=%s, `is_scraped`=%s "
                       "WHERE `id`=%s")
                #for abstract, exception, is_scraped, id_ in output:
                cur.execute(sql, (abstract, exception, is_scraped, id_))
                connection.commit()

if __name__ == "__main__":
    import os
    params = dict(url=os.environ["url_TO_SCRAPE"],
                  id=os.environ["id_TO_SCRAPE"])
    execute(**params)
