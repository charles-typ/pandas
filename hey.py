import pywren
from jiffy import JiffyClient
import logging

pywren.wrenlogging.default_config('INFO')
logging.basicConfig(level = logging.DEBUG)
logger = logging.getLogger(__name__)

def run_command(key):
	host = '172.31.12.102'
	logger.info("Connecting to the jiffy server")
	em = JiffyClient(host)
	logger.info("Jiffy connected")
	data_path = "/test"
	data_size = 30 * 1024 * 1024
	sample_data = 'a' * data_size
	test_queue = em.open_or_create_queue(data_path,"local://tmp", 10,1)
	logger.info("queue created")
	test_queue.put(sample_data)
	logger.info("Data put")
	obj = test_queue.get()
	logger.info("Data read: " + str(len(obj)))
	em.close(data_path)
	return 0	



wrenexec = pywren.default_executor()
keylist = [1]
futures = wrenexec.map(run_command, keylist)

pywren.wait(futures)
results = [f.result() for f in futures]
