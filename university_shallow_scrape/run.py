from webscraping.shallow_network_scrape import NetworkScrape
import ast
import boto3
import os
    

def execute(**params):
    depth = params.pop("depth")
    keywords = ast.literal_eval(params.pop('keywords'))
    outfile = params.pop("id")+".d"+depth+".pickle"
    cpu = 1
    if "cpu" in params:
        cpu = int(params["cpu"])

    ns = NetworkScrape(max_depth=int(depth), kw_depth=1,
                       keywords=keywords, n_proc=cpu, **params)
    print("Running")
    ns.run()
    print("Pickling")
    ns.pickle(outfile)
    
    # Send the data  
    print("Saving")                                             
    s3 = boto3.resource('s3')
    data = open(outfile, 'rb')
    s3.Bucket('network-scrape').put_object(Key=outfile, Body=data)
    os.remove(outfile)
    

if __name__ == "__main__":
    params = dict(top_url=os.environ["top_url_TO_SCRAPE"],
                  id=os.environ["id_TO_SCRAPE"],
                  depth=os.environ["depth_TO_SCRAPE"],
                  keywords=os.environ["keywords_TO_SCRAPE"])
    if "cpu_url_TO_SCRAPE" in os.environ:
        params["url"] = os.environ["cpu_url_TO_SCRAPE"]
    execute(**params)
