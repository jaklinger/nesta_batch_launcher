from webscraping.bulk_scrape import RequestsScraper
import pymysql
import json
import os
import re

DBCONF="innovation-mapping-tier0.config"
def execute(**params):
    js_data = '{}'
    exception = ""
    try:
        # Get the data
        rs = RequestsScraper(max_depth=3)
        rs.recursive_extraction(url=params["url"])
        print("Got",len(rs.all_texts),"items from",params["url"])
        js_data = json.dumps(rs.data)
    except Exception as err:
        #exception = re.sub('[^0-9a-zA-Z]+', ' ', str(err))
        #print("==>",exception)
        exception = str(err)

    # Write to database
    connection = pymysql.connect(read_default_file=DBCONF)
    with connection.cursor() as cur:
        sql = ("INSERT INTO `mexico_denue_website_text` "
               "(`id`, `visible_text`, `exception_text`) "
               "VALUES (%s, %s, %s)")
        cur.execute(sql, (params['id'], js_data, exception))
    connection.commit()

if __name__ == "__main__":
    params = dict(url=os.environ["URL_TO_SCRAPE"],
                  id=os.environ["URL_ID_NUMBER"])
    execute(**params)
