#!/usr/bin/python
# -*- encoding: utf-8 -*-
#!/usr/bin/python
# -*- encoding: utf-8 -*-

import json
from customDataFilterer import DataFilterer
from customBotoHandler import AdvancedBotoHandler
import boto3
from customLiterals import UrlLiteral
from email_mime import send_email_with_attachment 
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



def get_s3_params(s3_uri:str = ''):
    try:

        #s3_uri => s3://mybucketname/DataFolder1/DataFolder2/take-home.json
        full_path = s3_uri.split(r'//')[-1] # mybucketname/DataFolder1/DataFolder2/take-home.json
        bucket_name = full_path.split('/')[0] # mybucketname
        file_path =  '/'.join(full_path.split('/')[1 : ]) # /DataFolder1/DataFolder2/take-home.json
        return bucket_name, file_path
    except Exception as _ex:
        print(_ex)

def receive_s3_uri(botoHandler: AdvancedBotoHandler = None):
    try:
        messages = botoHandler.receive_messages(max_number = 1, wait_time = 15)
        return messages.get('Messages')[0].get('Body')
    except Exception as _ex:
        print(_ex)

def main():
    try:
       
        customBoto = AdvancedBotoHandler()
        s3_cli = customBoto.s3_client

        customBoto.send_message(message = 's3://dementeewdmyrtoigorovitch/DataFolder/take-home.json')
        
        #Read message from SQS queue with a link to an object in S3 bucket
        bucket_name, file_path = get_s3_params(receive_s3_uri(customBoto))

        #Get data from file (as a JSON), parse it, and get tag(s) ending with 1
        
        # download file from S3 bucket
        local_file_path = 'DataFolder/1.json'
        with open(local_file_path, 'wb') as f:
            s3_cli.download_fileobj(bucket_name, file_path, f)
        
        # parse data from file and get tag(s) ending with 1
        parsed_data = run_parse(local_file_path)

        # write parsed data to new file
        new_file_path = 'DataFolder/Filtered1.json'
        with open(new_file_path, 'w') as f:
            json.dump(parsed_data, f, ensure_ascii=False, indent=4)
        
        # upload new file to S3 bucket
        with open(new_file_path, "rb") as f:
            s3_cli.upload_fileobj(f, bucket_name, new_file_path)

        # send message with S3 URI pointer on file
        url = f'https://{bucket_name}.s3.{customBoto.region_name}.amazonaws.com/{new_file_path}'
        s3_uri = f's3://{bucket_name}/{new_file_path}'
        customBoto.send_message(message = s3_uri)
        
        email_adress = [
            'acuta.pugione@gmail.com',
            ]
        body_text = f'''This letter is the result of an application that filtered the data from the "{file_path}" file and saved it to the "{new_file_path}" file.
        Please review the attachment and make sure it is correct.'''
        
        send_email_with_attachment(
            email_src = 'acuta.pugione@gmail.com',
            emails_dest = email_adress,
            subject_text = f'Filtered file in {bucket_name}',
            body_text = body_text,
            file_path = new_file_path
            )
    except Exception as _ex:
        print(_ex)
    
    


if __name__ == "__main__":
    main()
