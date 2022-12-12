import datetime
import functools
import logging
import logging.handlers
import pika
import pika.credentials
import pika.spec
import signal
import threading
import time
import sys

# log_format = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) -35s %(lineno) -5d: %(message)s')
# log_formatter = logging.Formatter(log_format)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

file_handler = logging.handlers.WatchedFileHandler(filename="pika-1402.log")
# file_handler.setFormatter(log_formatter)
logger.addHandler(file_handler)

stdout_handler = logging.StreamHandler(sys.stdout)
# stdout_handler.setFormatter(log_formatter)
logger.addHandler(stdout_handler)

# %% PARAMETERS
RabbitMQ_host = "localhost"
RabbitMQ_port = 5672
RabbitMQ_queue = "test_ctrlC"
RabbitMQ_cred_un = "guest"
RabbitMQ_cred_pd = "guest"

# %% init variables for batch process
exiting = False
work_threads = list()
init_time = time.time()

# %% Function Message Acknowledgement
def ack_message(ch, delivery_tag):
    """Note that `ch` must be the same pika channel instance via which
    the message being ACKed was retrieved (AMQP protocol constraint).
    """
    logger.info(
        "DEBUG ack_message : begining of ack_message function, tag: %s", delivery_tag
    )

    if ch.is_open:
        ch.basic_ack(delivery_tag)
        logger.info(f"DEBUG ack_message : Acknowledgement delivered")
    else:
        # Channel is already closed, so we can't ACK this message;
        # log and/or do something that makes sense for your app in this case.
        now_str = str(datetime.datetime.now())
        delta_str = str(datetime.timedelta(seconds=time.time() - init_time))
        logger.error(
            "Channel Closed when trying to Acknowledge, now %s delta %s",
            now_str,
            delta_str,
        )
        pass


# %% Function Process multiple messages in separate thread
def do_work(channel, delivery_tag, body):
    logger.info(
        "DEBUG do_work : start of block_process function, tag: %s", delivery_tag
    )

    # do something that take time with the block of nessage in body_list
    time.sleep(5)

    body_str = body.decode()
    logger.info(f"DEBUG do_work : message processed is {body_str}")

    cb = functools.partial(ack_message, channel, delivery_tag)
    channel.connection.add_callback_threadsafe(cb)

    logger.info(f"DEBUG block_process : end of block_process function")


def process_message(ch, delivery_tag, body):
    global work_threads
    t = threading.Thread(target=do_work, args=(ch, delivery_tag, body))
    t.start()
    work_threads.append(t)


def pika_runner():
    # %% connect to RabbitMQ via Pika
    cred = pika.credentials.PlainCredentials(RabbitMQ_cred_un, RabbitMQ_cred_pd)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=RabbitMQ_host, port=RabbitMQ_port, credentials=cred
        )
    )
    channel = connection.channel()
    channel.queue_declare(queue=RabbitMQ_queue, durable=True)
    # tell rabbitMQ to don't dispatch a new message to a worker until it has processed and acknowledged the previous one :
    channel.basic_qos(prefetch_count=1)

    # %% empty queue and generate test data
    channel.queue_purge(queue=RabbitMQ_queue)
    # wait few second so the purge can be check in the RabbitMQ ui
    logger.info(f"DEBUG main : queue {RabbitMQ_queue} purged, sleeping 5 seconds")
    connection.sleep(5)
    logger.info(f"DEBUG main : done sleeping 5 seconds")
    # generate 10 test messages
    for msgId in range(10):
        channel.basic_publish(
            exchange="",
            routing_key=RabbitMQ_queue,
            body=f"message-{msgId}",
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
            ),
        )
    logger.info(f"DEBUG main : test messages created in {RabbitMQ_queue}")

    for method_frame, properties, body in channel.consume(
        queue=RabbitMQ_queue, inactivity_timeout=1
    ):
        if exiting:
            logger.info(f"DEBUG : stopping consuming")
            channel.stop_consuming()
            logger.info(f"DEBUG : joining work threads")
            for thread in work_threads:
                thread.join()
            logger.info(f"DEBUG : all work threads done, sleeping 5 seconds to let acks be delivered")
            connection.sleep(5)
            logger.info(f"DEBUG : closing connections and channels")
            channel.close()
            connection.close()
        else:
            if method_frame is not None:
                process_message(channel, method_frame.delivery_tag, body)


pika_thread = threading.Thread(target=pika_runner)
pika_thread.start()


# %% Function handle exit signals
def exit_handler(signum, _):
    global exiting
    if exiting:
        logger.info(f"DEBUG : already exiting")
        return
    exiting = True
    now_str = str(datetime.datetime.now())
    delta_str = str(datetime.timedelta(seconds=time.time() - init_time))
    logger.info("Exit signal received (%d) at %s, delta %s", signum, now_str, delta_str)
    for thread in work_threads:
        thread.join()
    pika_thread.join()
    exit(0)


signal.signal(signal.SIGINT, exit_handler)  # send by a CTRL+C or modified Docker Stop
# signal.signal(signal.SIGTSTP, exit_handler) # send by a CTRL+Z Docker Stop
logger.info(" [*] Waiting for messages. To exit press CTRL+C")
for thread in work_threads:
    thread.join()
pika_thread.join()

# %% ISSUES
# FIXME : CtrlC freeze the channel that don't process the .connection.add_callback_threadsafe during the thread.join()
