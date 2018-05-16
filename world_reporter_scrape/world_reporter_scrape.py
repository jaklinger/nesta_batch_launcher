from launcher import Launcher
import pymysql
import boto3

DBCONF='/home/ec2-user/db_config/innovation-mapping-tier0.config'

class WorldReporterLauncher(Launcher):
    def __init__(self, batch_size=100, *args, **kwargs):
        self.batch_size = batch_size
        super().__init__(*args, **kwargs)
        
    def prepare(self):
        connection = pymysql.connect(read_default_file=DBCONF)
        s3 = boto3.resource('s3')
        params_collection = []
        with connection.cursor() as cursor:
            sql = ("SELECT `id`, `url_abstract` FROM `world_reporter_grants` "
                   "WHERE `is_scraped` = 0")
            cursor.execute(sql)
            results = cursor.fetchmany(size=self.batch_size)
            while results:
                # Upload the metadata to S3
                body = ""
                ids = []
                for id_, url in results:
                    ids.append(str(id_))
                    body += str(id_)+","+url+"\n"
                file_name = "WorldReporterLauncher-"+ids[0]+"_"+ids[-1]+".csv"
                obj = s3.Object("nesta-inputs", file_name).put(Body=body.encode("utf-8"))
                # Store the filename
                params_collection.append(dict(url=file_name, id=None))
                results = cursor.fetchmany(size=self.batch_size)
        print("Got", len(params_collection), "parameters")
        return params_collection

    def execute(self,**params):
        pass


if __name__ == "__main__":
    # Files you want in compiled into your environment
    env_files=["/home/ec2-user/data_utils/webscraping", DBCONF,
               "/home/ec2-user/launchers/common"]
    # Launch the launcher
    mbl = WorldReporterLauncher(email_address="joel.klinger@nesta.org.uk", 
                                env_files=env_files, batch=True, batch_size=100)
    #mbl.run(max_runs=5, job_definition="phantom_js")
    mbl.run(job_definition="phantom_js")
