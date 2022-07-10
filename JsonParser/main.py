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

def step_by_step_application():
    try:
        # parse data from file
        data = parse_json_file("DataFolder/take-home.json").get("prices")
        
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
        data_filter = DataFilterer(data, filter_dictionary)

        # get filtered prices and filter
        filtered_data_prices, allowed_filter = data_filter.get_filtered  
        
        # show filtered prices
        print(filtered_data_prices)  
        # show filter
        print(allowed_filter.get('name'))  

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

def main():
    try:

        customBoto = bth()
        customBoto.send_message(message = 'Hello>>>\nMy name is Dmytro!\nI\'m so happy to see you!!!')
        
        messages = customBoto.receive_messages(max_number = 1, wait_time = 10)
        if messages:
            if messages.get('Messages'):
                for item in messages.get('Messages'):
                    if item.get('Body'):
                        ms_body = item.get('Body')
                        break
        url = first_url(ms_body)
        if url:
            print(url)

        #for mess in messages.get('Messages'):
        #    print(f'Message #{mess.get("MessageId")}\nBody:{mess.get("Body")}')
    except Exception as _ex:
        print(_ex)
    #del customBoto

    #delete_queues()
    
    #boto_handler.send_message(message = 'Hello>>>\nMy name is Dmytro!\nI\'m so happy to see you!!!')

    #messages = boto_handler.receive_messages(max_number = 10, wait_time = 15)
    #print(messages)
    #for mess in messages:
    #    print(f'Message #{mess.get("MessageId")}\nBody:{mess.body}')
    #    mess.delete()
    #step_by_step_application()


if __name__ == "__main__":
    main()
