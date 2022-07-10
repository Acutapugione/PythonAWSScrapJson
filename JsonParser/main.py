#!/usr/bin/python
# -*- encoding: utf-8 -*-
#!/usr/bin/python
# -*- encoding: utf-8 -*-

import json
from customDataFilterer import DataFilterer
from customBotoHandler import AdvancedBotoHandler as bth
import boto3
from customLiterals import UrlLiteral
import re
  
def first_url(text:str = ""):
    urls = re.findall(UrlLiteral, text)      
    return [x[0] for x in urls]

def parse_json_file(file_path=""):

    try:
        with open(file_path, "r", encoding="utf-8") as m_file:
                return json.load(m_file)
    except Exception as _ex:
        print(_ex)

def run_parse(file_path:str = ''):
    try:
        # parse data from file
        data = parse_json_file(file_path)
        prices = data.get("prices")
        #data = parse_json_file("DataFolder/take-home.json").get("prices")
        
        # create filter dictionary with target and rule options
        filter_dictionary = {  
            "name": "Last tag symbol must be \'1\'",
            "target": "tag",
            "rule": lambda val: val[-1] == "1",
        }

        """ 
        create custom class object
        with part of data 'prices' and filter dictionary
        """
        data_filter = DataFilterer(prices, filter_dictionary)

        # get filtered prices and filter
        filtered_data_prices, allowed_filter = data_filter.get_filtered  
        
        # show filtered prices
        print(filtered_data_prices)  
        # show filter
        print(allowed_filter.get('name'))  
        data["prices"] = filtered_data_prices
        return data

    except Exception as _ex:
        print(_ex)

def delete_queues():
    """
    import boto3
    client = boto3.client('sqs')
    q = client.create_queue(QueueName='foo')
    #{ Some code here }
    
    client.delete_queue(QueueUrl=q['QueueUrl'])
    """
    queues = boto3.resource('sqs').queues.all()
    for queue in queues:
        boto3.client('sqs').delete_queue(QueueUrl=queue.url)#'QueueUrl']
        print(f'del queue url{queue.url}')

def get_s3_params(s3_uri:str = ''):
    try:

        #s3_uri => s3://mybucketname/DataFolder1/DataFolder2/take-home.json
        full_path = s3_uri.split(r'//')[-1] # mybucketname/DataFolder1/DataFolder2/take-home.json
        bucket_name = full_path.split('/')[0] # mybucketname
        file_path =  '/'.join(full_path.split('/')[1 : ]) # /DataFolder1/DataFolder2/take-home.json
        return bucket_name, file_path
    except Exception as _ex:
        print(_ex)

def receive_s3_uri(botoHandler: bth = None):
    try:
        messages = botoHandler.receive_messages(max_number = 1, wait_time = 15)
        return messages.get('Messages')[0].get('Body')
    except Exception as _ex:
        print(_ex)

def main():
    try:
        s3_cli = boto3.client('s3')
        customBoto = bth()
        customBoto.send_message(message = 's3://dementeewdmyrtoigorovitch/DataFolder/take-home.json')
        
        #Read message from SQS queue with a link to an object in S3 bucket
        bucket_name, file_path = get_s3_params(receive_s3_uri(customBoto))

        #Get data from file (as a JSON), parse it, and get tag(s) ending with 1
        with open('DataFolder/1.json', 'wb') as f:
            s3_cli.download_fileobj(bucket_name, file_path, f)
        
        # parse data from file
        parsed_data = run_parse('DataFolder/1.json')
        with open('DataFolder/Filtered1.json', 'w') as f:
            json.dump(parsed_data, f, ensure_ascii=False, indent=4)
        
        
        with open("DataFolder/Filtered1.json", "rb") as f:
            s3_cli.upload_fileobj(f, bucket_name, 'Filtered1.json')
        
    except Exception as _ex:
        print(_ex)
    
    


if __name__ == "__main__":
    main()
