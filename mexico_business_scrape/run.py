from webscraping.bulk_scrape import RequestsScraper
import pymysql
import json
import os

DBCONF="innovation-mapping-tier0.config"
def execute(**params):
    # Get the data
    rs = RequestsScraper(max_depth=3)
    rs.recursive_extraction(url=params["url"])
    print("Got",len(rs.all_texts),"items from",params["url"])
    
    # Write to database
    js_data = json.dumps(rs.data)
    connection = pymysql.connect(read_default_file=DBCONF)
    with connection.cursor() as cur:
        sql = ("INSERT INTO `mexico_denue_website_text` (`id`, `visible_text`) "
               "VALUES (%s, %s)")
        cur.execute(sql, (params['id'], js_data))
    connection.commit()

if __name__ == "__main__":
    params = dict(url=os.environ["URL_TO_SCRAPE"],
                  id=os.environ["URL_ID_NUMBER"])
    execute(**params)
