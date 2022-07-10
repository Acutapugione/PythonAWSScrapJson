
#!/usr/bin/python
# -*- encoding: utf-8 -*-
try:
    print('*'*80)
    print('Please, wait for the libraries to be ready...')
    

    from random import randint
    from datetime import datetime
    import boto3
    import socket


    print('Libraries successfully prepared!')



    class CustomBotoHandler:
        """
        This class provides advanced capabilities for using boto3.
        There is control over the generated queue resources.
        """


        def __init__(self, s3_res = None, sqs_res = None, queue_res = None, sqs_client = None):
            try:
                self._generated_queues = []
                if s3_res is not None:
                    self._s3_res = s3_res
                else:
                    self._s3_res = boto3.resource('s3')
                if sqs_res is not None:
                    self._sqs_res = sqs_res
                else:
                    self._sqs_res = boto3.resource('sqs')
                if queue_res is not None:
                    self._queue_res = queue_res
                else:
                    self._queue_res = self.generate_queue_res(delay = 15)
                
                if sqs_client is not None:
                    self._sqs_client = sqs_client
                else:
                    self._sqs_client = boto3.client('sqs')
                
            except Exception as _ex:
                print(f'{_ex} in {self.__class__.__name__}')

        def __del__(self):
            print(f'Del queue res - {self.queue_res.url.split("/")[-1].strip()}')
            del self.queue_res
            print(f'Del sqs client - {self.sqs_client}')
            del self.sqs_client #>>> del self.generated_queues
            print(f'Del sqs resource - {self.sqs_res}')
            del self.sqs_res 
            print(f'Del s3 resource - {self.s3_res}')
            del self.s3_res

        # PROPERTY sqs_client
        @property 
        def sqs_client(self): 
            return self._sqs_client

        @sqs_client.setter
        def sqs_client(self, client = None):
            if client is not None:
                self._sqs_client = client

        @sqs_client.deleter
        def sqs_client(self):
            print('Deleting generated queues...')
            del self.generated_queues
            print('Done')
            del self._sqs_client

        # PROPERTY sqs_res
        @property
        def sqs_res(self):
            return self._sqs_res

        @sqs_res.setter
        def sqs_res(self, new_sqs_res):
            try:
                if len(new_sqs_res.list_queues(MaxResults = 1))==0:
                    raise ValueError('Its sqs with no queues')
                self._sqs_res = new_sqs_res
            except Exception as _ex:
                print(f'{_ex} in {self.__class__.__name__}')
        
        @sqs_res.deleter
        def sqs_res(self):
            del self._sqs_res

        # PROPERTY queue_res
        @property
        def queue_res(self, sqs_res:any = None, queue_name:str = 'test'):
            if self._queue_res is not None:
                return self._queue_res
            try:
                queue_name = queue_name.split('/')[-1].strip()
                if sqs_res is not None:
                    return sqs_res.get_queue_by_name(QueueName = queue_name)
                return self._sqs.get_queue_by_name(QueueName = queue_name)
            except Exception as _ex:
                print(f'{_ex} in {self.__class__.__name__}')
                self._queue_res_was_generated = True
                if sqs_res is not None:
                    return self.generate_queue_res(sqs_res, delay = 15)
                return self.generate_queue_res(delay = 15)
        
        @queue_res.setter
        def queue_res(self, new_queue):
            try:
                self._sqs_res.get_queue_url(QueueName=new_queue.url.split('/')[-1].strip())['QueueUrl']
                self._queue_res = new_queue
            except Exception as _ex:
                print(f'{_ex} in {self.__class__.__name__}')

        @queue_res.deleter
        def queue_res(self):
            try:
                del self._queue_res
            except Exception as _ex:
                print(f'{_ex} in {self.__class__.__name__}')


        # PROPERTY _generated_queues
        @property 
        def generated_queues(self):
            return [ x.url.split('/')[-1].strip() for x in self._generated_queues ]

        @generated_queues.setter
        def generated_queues(self, new_queues):
            try: 
                if len(new_queues) == 0:
                    raise ValueError(f'You passed an empty set: {new_queues}')
                for queue in new_queues:
                    if queue.url not in self._generated_queues:
                        self._generated_queues.append(queue.url)
            except Exception as _ex:
                print(f'{_ex} in {self.__class__.__name__}')

        @generated_queues.deleter
        def generated_queues(self):
            counter = 0
            total = len(self._generated_queues)

            #queues = boto3.resource('sqs').queues.all()
            #for queue in queues:
            #    boto3.client('sqs').delete_queue(QueueUrl=queue.url)#'QueueUrl']
            #    print(f'del queue url{queue.url}')

            for queue_url in self._generated_queues:
                counter =+ 1
                try:
                    print(f'Remove {counter}st object out of {total}.')
                    self._sqs_client.delete_queue(QueueUrl=queue_url)    
                except Exception as _ex:
                    print(f'Object {counter} could not be deleted.\nInner exception: {_ex}')

            del self._generated_queues


        # PROPERTY s3_res
        @property
        def s3_res(self):
            return self._s3_res  
        
        @s3_res.setter
        def s3_res(self, new_s3_res):
            try:
                buckets = new_s3_res.buckets.all()
                if len(buckets) == 0:
                    raise ValueError('Its s3 with no buckets')
                self._s3_res = new_s3_res
            except Exception as _ex:
                print(f'{_ex} in {self.__class__.__name__}')
        
        @s3_res.deleter
        def s3_res(self):
            del self._s3_res
       
        
        @property
        def buckets(self, s3_res = None):
            """Returns set of buckets from s3"""
            try:
                if s3_res is None:
                    return [self._s3_res.buckets.all()]
                return [s3_res.buckets.all()]
            except Exception as _ex:
                print(f'{_ex} in {self.__class__.__name__}')
               
        def generate_queue_res(self, sqs_res:any = None, delay:int = 15): 
            try:
                if delay <= 0 or delay > 60: 
                    raise ValueError(f'The passed "delay" parameter does not match the conditions:\n0<{delay}<=60')
                queue_name = f'{datetime.now().strftime("%d_%m_%Y_%H_%M_%S")}_{randint(100, 999)}'
                if sqs_res is not None:
                    queue = sqs_res.create_queue(
                        QueueName = queue_name, 
                        Attributes= {
                            'DelaySeconds': str(delay)
                            }
                        )
                    self.queue_generate_handler(queue)
                    return queue
                queue = self._sqs_res.create_queue(
                    QueueName = queue_name, 
                    Attributes= {
                        'DelaySeconds': str(delay)
                        }
                    )
                self.queue_generate_handler(queue)
                return queue
            except Exception as _ex:
                print(f'{_ex} in {self.__class__.__name__}')

        def queue_generate_handler(self, queue = None):
            if queue is not None and queue.url not in self._generated_queues:
                self._generated_queues.append(queue.url)
    
    class AdvancedBotoHandler(CustomBotoHandler):
        """
        This class extends the capabilities of the CustomBotoHandler. 
        There are exception-resistant methods for sending and receiving messages.
        """

        
        def __init__(self, s3_res = None, 
                        sqs_res = None, queue_res = None):
            super().__init__( s3_res, sqs_res, queue_res)
        def __del__(sel):
            super().__del__()
        def send_message(self, queue = None, queue_name:str = '', message:any = {}, indx = 0):
            try:
                if indx > 1:
                    raise Exception(f'Some error in {self.__class__.__name__}.send_message()!')
                if queue is not None:
                    if isinstance(message, dict):
                        return queue.send_message(
                            MessageBody = message.get('body'),
                            MessageAttributes = message.get('attributes')

                            )
                    elif isinstance(message, (set, tuple, list)):
                        response = []
                        for mess in message:
                            response.append(
                                self.send_message(
                                    queue = queue, 
                                    message = mess
                                    )
                                )
                        return response
                    elif isinstance(message, str):
                        return self.send_message(
                            queue=queue,
                            message={ 
                                'body': message,
                                'attributes': {
                                        'Author': {
                                            'StringValue': f'{socket.gethostname()}',
                                            'DataType': 'String'
                                        }
                                    }
                                }
                            )
                elif len( queue_name ) > 0:
                    if indx >= 1: 
                        return self.send_message(
                            queue = super().queue_res,
                            message=message,
                            indx = indx + 1
                            )
                    return self.send_message( 
                        queue = super().sqs_res.get_queue_by_name(queue_name), 
                        message=message, indx = indx +1 
                        )
                else:
                    return self.send_message(
                        queue = super().queue_res,
                        message = message, 
                        indx = indx +1 
                        )
            except Exception as _ex:
                print(f'{_ex} in {self.__class__.__name__}')

        def receive_messages(self, queue = None, queue_name:str = '', max_number=10, wait_time=10):
            try:
                if queue is not None:
                    return super().sqs_client.receive_message(
                        QueueUrl = queue.url,
                        MaxNumberOfMessages = max_number,
                        WaitTimeSeconds = wait_time
                        )
                elif len( queue_name ) > 0:

                    return self.receive_messages(
                        queue = super().sqs_res.get_queue_by_name(queue_name),
                        max_number = max_number,
                        wait_time = max_number
                        )
                return super().sqs_client.receive_message(
                        QueueUrl = super().queue_res.url,
                        MaxNumberOfMessages = max_number,
                        WaitTimeSeconds = wait_time
                        )
            except Exception as _ex:
                print(f'{_ex} in {self.__class__.__name__}')
except Exception as _ex:
    print(f'{_ex} in {__name__}')      
    