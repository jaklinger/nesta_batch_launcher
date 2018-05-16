from abc import ABC
from abc import abstractmethod
import boto3
import datetime
import json
import shutil
import os
from subprocess import check_output

MASTER_EMAIL='joel.klinger@nesta.org.uk'

def exec_and_read(cmd):
    out = check_output(cmd,shell=True)
    return out.decode("utf-8").split("\n")[-2]

class Launcher(ABC):
    def __init__(self, email_address, env_files=[], batch=False, store_exceptions=True):
        self.email_address = email_address
        self.error_codes = {}
        self.timestamp = str(datetime.datetime.now().timestamp())
        self.n_input = 0
        self.n_exec = 0
        self.n_success = 0
        self.n_fail = 0
        self.batch = batch
        self.store_exceptions = store_exceptions
        self.env_files = " ".join(env_files)
        self.aws_id = exec_and_read(["aws --profile default configure"
                                     " get aws_access_key_id"])
        self.aws_secret = exec_and_read(["aws --profile default configure"
                                         " get aws_secret_access_key"])

    def run(self, max_runs=None, job_definition="test_definition", cpu=None,
            #container_name=None,
            offset=None):
        '''prepare, execute and finalise'''
        params_collection = self.prepare()
        if offset is None:
            offset = 0
        if max_runs is None:
            max_runs = len(params_collection)
        elif offset is not None:
            max_runs = max_runs + offset

        # Create the environment on S3 for batching
        batch_file_timestamp = None
        if self.batch:
            shutil.copy("/home/ec2-user/bash_scripts/prepare_environment.sh",
                        os.getcwd())
            out = check_output(["./prepare_environment.sh "+self.env_files], shell=True)
            os.remove("prepare_environment.sh")
            batch_file_timestamp = out.decode("utf-8").split("\n")[-2]
            client = boto3.client('batch')            

        # If specific CPU requirement is specified
        bonus = {}
        if cpu is not None:
            #err_msg = "Container name must be specified if overriding cpu number"
            #assert container_name is not None, err_msg
            bonus = dict(vcpus=cpu) #, name=container_name)            
        
        for i, params in enumerate(params_collection):
            if i < offset:
                continue
            if i >= max_runs:
                break
            self.n_input += 1
            # if params['done']:
            #     continue
            error_code = None
            if cpu is not None:
                params["cpu"] = cpu

            if self.batch:
                env = [{"name":"AWS_ACCESS_KEY_ID", "value":self.aws_id},
                       {"name":"AWS_SECRET_ACCESS_KEY", "value":self.aws_secret},
                       {"name":"FILE_TIMESTAMP", "value":batch_file_timestamp},
                       ]
                #       {"name":"URL_TO_SCRAPE", "value":params['url']},
                #       {"name":"URL_ID_NUMBER", "value":str(params["id"])}]
                # 
                for k,v in params.items():
                    new_row = dict(name=k + "_TO_SCRAPE", value=str(v))
                    env.append(new_row)
                    
                # Should generate a unique job name, with the batch number included
                response = client.submit_job(jobDefinition=job_definition,
                                             jobName='test_submission',
                                             jobQueue='HighPriority',
                                             containerOverrides={"environment":env,
                                                                 **bonus})
                if i % 1000 == 0:
                    print("%d: %s" % (i," so far"))
                #print(params['url'], response)
            else:
                try:
                    self.n_exec += 1
                    self.execute(**params)
                except Exception as err:
                    if not self.store_exceptions:
                        raise err
                    self.n_fail += 1 
                    self.error_codes[params["id"]] = str(err)
                else:
                    self.n_success += 1
        #self.finalise()

    @abstractmethod
    def prepare(self):
        '''
        Must return a list of parameters (dict) containing at least the keys:
        
        - id : unique id of the execution. Useful for joining results.
        - done : bool indicating whether the id has previously been processed.
        '''
        pass

    @abstractmethod
    def execute(self, **params):
        '''
        Execute the main code on a single parameter set.

        - params: A single row from the output of self.prepare. 
                  Can contain parameters required for this execution.
        '''
        pass

    def finalise(self):
        # Save error log to S3
        s3 = boto3.resource('s3')
        obj = s3.Object("nesta-errors", self.timestamp+".err")
        obj.put(Body=json.dumps(self.error_codes))

        # Send email to summary of number {inputs, execs, successes, fails}
        ses = boto3.client('ses', region_name='eu-west-1')
        email_text = (
            '''\tHello,

            Job {} has finished running. Summary of the run:

            \tInputs:\t {},
            \tExecutions:\t {},
            \tSuccesses:\t {},
            \tFails:\t {}
            
            All the best,
            Automatic Joel 
            (please don't reply)
            '''.format(self.timestamp, self.n_input, 
                       self.n_exec, self.n_success, self.n_fail)
        )
        ses.send_email(Source=MASTER_EMAIL,
                       Destination={'ToAddresses':[self.email_address]},
                       Message={
                           'Subject': dict(Data='Job '+self.timestamp+" completed"),
                           'Body': dict(Text=dict(Data=email_text))
                       })

