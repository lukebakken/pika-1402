import datetime
import functools
import logging
import pika
import pika.credentials
import pika.spec
import signal
import threading
import time
import traceback

from queue import Queue

log_format = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) -35s %(lineno) -5d: %(message)s')
logger = logging.getLogger(__name__)

logging.basicConfig(level=logging.INFO, format=log_format)

# %% PARAMETERS
RabbitMQ_host = 'localhost'
RabbitMQ_port = 5672
RabbitMQ_queue = 'test_ctrlC'
RabbitMQ_cred_un = 'guest'
RabbitMQ_cred_pd = 'guest'

# %% init variables for batch process
list_Boby_Tag = Queue()
threads = list()
event = None
PauseConsume = False
init_time = time.time()

# %% Function Message Acknowledgement
def ack_message(ch, delivery_tag):
    """Note that `ch` must be the same pika channel instance via which
    the message being ACKed was retrieved (AMQP protocol constraint).
    """
    logger.info(f'DEBUG ack_message : begining of ack_message function')

    if ch.is_open:
        ch.basic_ack(delivery_tag)
        logger.info(f'DEBUG ack_message : Acknowledgement delivered')
    else:
        # Channel is already closed, so we can't ACK this message;
        # log and/or do something that makes sense for your app in this case.
        logger.error(datetime.datetime.now(),str(datetime.timedelta(seconds=time.time() - init_time)),f'ERROR Channel Closed when trying to Acknowledge')
        pass

# %% Function Process multiple messages in separate thread 
def block_process():
    # list global variables to be changed
    global channel
    # init local variables
    body_list = list()
    tag_list = list()

    print(f'DEBUG block_process : start of block_process function')

    # cancel the timer if exist, as we will proces all elements in the queue here
    if event and event.is_alive():
        event.cancel()

    # extract all queued messages fom internal python queue and rebuild individual listes body and tag from tupple
    for _ in range(list_Boby_Tag.qsize()):
        myTuppleBodyTag = list_Boby_Tag.get()
        body_list += [myTuppleBodyTag[0]]
        tag_list += [myTuppleBodyTag[1]]
    # that also empty the queue

    # do something that take time with the block of nessage in body_list
    time.sleep(10)
    for body in body_list:
        body_str = body.decode()
        logger.info(f'DEBUG block_process : message processed is {body_str}')

    # acknowledging all tags in tag_list by using the channel thread safe function .connection.add_callback_threadsafe
    for tag in tag_list:
        logger.info(f'DEBUG preprare delivering Acknowledgement from thread')
        cb = functools.partial(ack_message, channel, tag)
        channel.connection.add_callback_threadsafe(cb)

    logger.info(f'DEBUG block_process : end of block_process function')

    return

# %% Function Process message by message and call 
def process_message(ch, method, _properties, body):
    # list global variables to be changed
    global list_Boby_Tag
    global event
    global threads

    # do nothing if this flag is on, as the program is about to close
    if PauseConsume:
        logger.info(f'DEBUG process_message : PauseConsume is True')
        return
    
    # cancel the timer if exist as we are going to process a block or restart a new timer
    if event and event.is_alive():
        event.cancel()

    # put in the queue the data from the body and tag as tupple
    list_Boby_Tag.put((body,method.delivery_tag))

    # if a max queue size is reached (here 500), immmediately launch a new thread to process the queue
    if list_Boby_Tag.qsize() == 500 :
        #print(f'DEBUG thread count before {len(threads)}')
        # keep in the threads list only the thread still running
        threads = [x for x in threads if x.is_alive()]
        #print(f'DEBUG thread count after {len(threads)}')
        # start the inference in a separated thread
        t = threading.Thread(target=block_process)
        t.start()
        # keep trace of the thread so it can be waited at the end if still running
        threads.append(t)
        #print(f'DEBUG thread count after add {len(threads)}')
    elif list_Boby_Tag.qsize() > 0 :
        # if the queue is not full create a thread with a timer to do the process after sometime, here 10 seconds for test purpose
        event = threading.Timer(interval=10, function=block_process)
        event.start()
        # also add this thread to the list of threads
        threads.append(event)

# %% connect to RabbitMQ via Pika
cred = pika.credentials.PlainCredentials(RabbitMQ_cred_un,RabbitMQ_cred_pd)
connection = pika.BlockingConnection(pika.ConnectionParameters(host=RabbitMQ_host, port=RabbitMQ_port, credentials=cred))
channel = connection.channel()
channel.queue_declare(queue=RabbitMQ_queue,durable=True)
# tell rabbitMQ to don't dispatch a new message to a worker until it has processed and acknowledged the previous one :
channel.basic_qos(prefetch_count=1)

# %% define the comsumer
channel.basic_consume(queue=RabbitMQ_queue,
                      auto_ack=False, # false = need message acknowledgement : basic_ack in the callback
                      on_message_callback=process_message)

# %% empty queue and generate test data
channel.queue_purge(queue=RabbitMQ_queue)
# wait few second so the purge can be check in the RabbitMQ ui
logger.info(f'DEBUG main : queue {RabbitMQ_queue} purged, sleeping 5 seconds')
connection.sleep(5)
logger.info(f'DEBUG main : done sleeping 5 seconds')
# generate 10 test messages
for msgId in range(10):
    channel.basic_publish(exchange='',
                        routing_key=RabbitMQ_queue,
                        body=f'message{msgId}',
                        properties=pika.BasicProperties(
                            delivery_mode = pika.spec.PERSISTENT_DELIVERY_MODE
                        ))
logger.info(f'DEBUG main : test messages created in {RabbitMQ_queue}')

# %% Function clean stop of pika connection in case of interruption or exception
def cleanClose():
    channel.stop_consuming()
    # tell the on_message_callback to do nothing 
    PauseConsume = True
    # Wait for all threads to complete
    for thread in threads:
        thread.join()
    # stop pika connection after a short pause
    connection.sleep(3)
    connection.close()
    return

# %% Function handle exit signals
def exit_handler(signum, _frame):
    now_str = str(datetime.datetime.now())
    delta_str = str(datetime.timedelta(seconds=time.time() - init_time))
    logger.info('Exit signal received (%d) at %s, delta %s', signum, now_str, delta_str)
    cleanClose()
    exit(0)

signal.signal(signal.SIGINT, exit_handler) # send by a CTRL+C or modified Docker Stop
#signal.signal(signal.SIGTSTP, exit_handler) # send by a CTRL+Z Docker Stop

print(' [*] Waiting for messages. To exit press CTRL+C')
try:
    channel.start_consuming()
except Exception:
    now_str = str(datetime.datetime.now())
    delta_str = str(datetime.timedelta(seconds=time.time() - init_time))
    logger.error('Exception received within start_consumming at %s, delta %s', now_str, delta_str, exc_info=True)
    cleanClose()

# %% ISSUES
# FIXME : CtrlC freeze the channel that don't process the .connection.add_callback_threadsafe during the thread.join()
