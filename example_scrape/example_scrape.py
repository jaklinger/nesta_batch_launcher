from launcher import Launcher
import boto3


class ExampleLauncher(Launcher):
    def prepare(self):
        # Example of reading data from s3
        s3 = boto3.resource('s3')    
        obj = s3.Object("nesta-inputs", "some-input-file")
        obj_io = obj.get()['Body'].read()        
        # Generate a collection of parameters
        params_collection = [dict(param_1="a value",
                                  param_2="another value")
                                  row in something]
        print("Got",len(params_collection),"parameters")
        # Read output database to see whether ids has already been processed
        return params_collection

    def execute(self,**params):
        pass


if __name__ == "__main__":
    # Files you want in compiled into your environment
    env_files=["/home/ec2-user/data_utils/webscraping",
               "/home/ec2-user/db_config/innovation-mapping-tier0.config"]
    # Launch the launcher
    mbl = ExampleLauncher(email_address="<your_email>", 
                          env_files=env_files, batch=True)
    mbl.run() #max_runs=10)
