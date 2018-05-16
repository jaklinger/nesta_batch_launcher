from launcher import Launcher
import boto3
import pandas as pd
import io
from run import execute as run_execute

from urllib.parse import urlsplit
import re

class NetworkScrapeLauncher(Launcher):
    def prepare(self):
        s3 = boto3.resource('s3')    
        obj = s3.Object("nesta-inputs", "university_urls.csv")
        obj_io = obj.get()['Body'].read()

        # 
        df = pd.read_csv(io.BytesIO(obj_io))
        condition = df.country_code.apply(lambda x: x in ["GB", "IT"])
        df = df.loc[condition]

        keywords_en = ['courses', 'school', 'department']
        keywords_it = ['cors','scuol', 'dipartiment']
        keywords = {}
        keywords['IT'] = str(keywords_it+keywords_en)
        keywords['GB'] = str(keywords_en)

        params_collection = []
        for _,row in df.iterrows():
            url = row["link"]
            if not re.match(r'http(s?)\:', url):
                url = 'http://' + url
            parsed = urlsplit(url)
            host = parsed.netloc
            if host.startswith('www.'):
                host = host[4:]
            params = dict(id=row["ID"], depth="4",
                          keywords=keywords[row.country_code],
                          top_url=url, url_substring=host)
            params_collection.append(params)

        print("Got",len(params_collection),"parameters")
        # Read output database to see whether ids has already been processed
        return params_collection

    def execute(self,**params):
        run_execute(**params)

        # outfile = params.pop("title")+".d4.pickle"
        # ns = NetworkScrape(max_depth=4, kw_depth=1, 
        #                    keywords=['dipartiment','courses',
        #                              'department','scuol',
        #                              'school','cors'], n_proc=4,
        #                    **params)
        # ns.run()
        # ns.pickle(outfile)

        # # Send the data
        # s3 = boto3.resource('s3')
        # data = open(outfile, 'rb')
        # s3.Bucket('network-scrape').put_object(Key=outfile, Body=data)


if __name__ == "__main__":
    env_files=["/home/ec2-user/data_utils/webscraping"]
    mbl = NetworkScrapeLauncher(email_address="joel.klinger@nesta.org.uk", 
                                env_files=env_files, batch=True, store_exceptions=False)
    mbl.run(max_runs=1, offset=613, cpu=8) #, container_name="default")
