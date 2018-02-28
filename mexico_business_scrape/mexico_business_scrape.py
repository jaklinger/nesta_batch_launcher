from webscraping.bulk_scrape import RequestsScraper
from launcher import Launcher

import boto3
import pandas as pd
import io
import pymysql
import json
import requests

DBCONF="/home/ec2-user/db_config/innovation-mapping-tier0.config"

class MexicoBizLauncher(Launcher):
    def prepare(self):
        s3 = boto3.resource('s3')    
        obj = s3.Object("nesta-inputs", "denue_website_data.csv")
        obj_io = obj.get()['Body'].read()
        # Get the connection to the database
        connection = pymysql.connect(read_default_file=DBCONF)
        with connection.cursor() as cur:
            sql = "SELECT `id` FROM `mexico_denue_website_text`;"
            cur.execute(sql)
            all_ids = set(x[0] for x in cur.fetchall())
        # Read into a dataframe and generate parameters
        df = pd.read_csv(io.BytesIO(obj_io))
        params_collection = [dict(id=row["id"], 
                                  url=row["website"], 
                                  done=int(row['id']) in all_ids)
                             for _,row in df.iterrows()]
        print("Got",len(params_collection),"parameters")
        # Read output database to see whether ids has already been processed
        return params_collection

    def execute(self,**params):
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
    # NEED THIS https://docs.aws.amazon.com/batch/latest/userguide/job_definitions.html
    mbl = MexicoBizLauncher(email_address="joel.klinger@nesta.org.uk", 
                            env_files=["/home/ec2-user/data_utils/webscraping",
                                       "/home/ec2-user/db_config/innovation-mapping-tier0.config"],
                            batch=True)
    mbl.run() #max_runs=10)
