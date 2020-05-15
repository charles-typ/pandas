import pandas as pd
import numpy as np
import os
import sys
import json
from multiprocessing import Manager
from multiprocessing.pool import ThreadPool
import pywren
import pandas.util.testing as tm

from pandas import DataFrame, MultiIndex, Series, concat, date_range, merge, merge_asof
from pandas import pipeline_merge
import pandas.util.testing as tm
import sys


try:
    from pandas import merge_ordered
except ImportError:
    from pandas import ordered_merge as merge_ordered


from multiprocessing import Process

import time
from hashlib import md5

from io import StringIO
import boto3
from io import BytesIO
from s3fs import S3FileSystem

from jiffy import JiffyClient

import logging
#logging.basicConfig(level = logging.INFO,format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# pywren.wrenlogging.default_config('INFO')
# logging.basicConfig(level = logging.INFO)
# logger = logging.getLogger(__name__)
pywren.wrenlogging.default_config('INFO')
logging.basicConfig(level = logging.DEBUG)
logger = logging.getLogger(__name__)

def read_s3_table(key, s3_client=None):
    loc = key['loc']
    names = list(key['names'])
    names.append("")
    dtypes = key['dtypes']
    parse_dates = []
#     for d in dtypes:
#         if dtypes[d] == datetime.datetime or dtypes[d] == np.datetime64:
#             parse_dates.append(d)
#             dtypes[d] = np.dtype("string")
    if s3_client == None:
        s3_client = boto3.client("s3")
    data = []
    if isinstance(key['loc'], str):
        loc = key['loc']
        obj = s3_client.get_object(Bucket='hong-tpcds-data', Key=loc[21:])['Body'].read()
#         print(sys.getsizeof(obj))
#         print(type(obj))
        data.append(obj)
    else:
        for loc in key['loc']:
            obj = s3_client.get_object(Bucket='hong-tpcds-data', Key=loc[21:])['Body'].read()
            data.append(obj)
#     part_data = pd.read_table(BytesIO("".join(data)),
#                               delimiter="|",
#                               header=None,
#                               names=names,
#                               usecols=range(len(names)-1),
#                               dtype=dtypes,
#                               na_values = "-",
#                               parse_dates=parse_dates)
#     #print(part_data.info())
#     return part_data
    return data



def hash_key_to_index(key, number):
    return int(md5(key).hexdigest()[8:], 16) % number

def my_hash_function(row, indices):
    # print indices
    #return int(sha1("".join([str(row[index]) for index in indices])).hexdigest()[8:], 16) % 65536
    #return hashxx("".join([str(row[index]) for index in indices]))% 65536
    #return random.randint(0,65536)
    return hash("".join([str(row[index]) for index in indices])) % 65536

def add_bin(df, indices, bintype, partitions):

    hvalues = df.apply(lambda x: my_hash_function(tuple(x), indices), axis = 1)

    if bintype == 'uniform':
        #_, bins = pd.qcut(samples, partitions, retbins=True, labels=False)
        bins = np.linspace(0, 65536, num=(partitions+1), endpoint=True)
    elif bintype == 'sample':
        samples = hvalues.sample(n=min(hvalues.size, max(hvalues.size/8, 65536)))
        _, bins = pd.qcut(samples, partitions, retbins=True, labels=False)
    else:
        raise Exception()
    #print("here is " + str(time.time() - tstart))
    #TODO: FIX this for outputinfo
    if hvalues.empty:
        return []

    df['bin'] = pd.cut(hvalues, bins=bins, labels=False, include_lowest=False)
    #print("here is " + str(time.time() - tstart))
    return bins

##########stageId is the source stage (to make it unique)
########这些queue 应该是可以从sender,receiver以及scheduleer都能打开的
############name is the identifier
def open_or_create_jiffy_queues(em, appName, partitions,stageId,role):    # multi writer
    if role == "sender":
        data_ques = [0]*partitions
        msg_que = []
        for reduceId in range(partitions):
            data_path = "/" + appName + "/" + str(stageId) +   "-" +  str(reduceId)
      #      data_path = "/" + key['appName'] + "/" +  str(reduceId)
            data_ques[reduceId] = em.open_or_create_queue(data_path,"local://tmp", 10,1)
#             print(reduceId)
#             print(data_path)
#             print(data_ques)
        msg_path = "/" + appName + "-msg" + "/" + str(stageId)
    #      msg_path = "/" + key['appName'] + "-msg" + "/" + str(reduceId)

        msg_que = em.open_or_create_queue(msg_path, "local://tmp", 1)
        return data_ques, msg_que

    elif role == "receiver":
        data_ques = [0]*partitions
        for reduceId in range(partitions):
            data_path = "/" + appName + "/" + str(stageId) +  "-" +  str(reduceId)
            data_ques[reduceId] = em.open_or_create_queue(data_path,"local://tmp", 10,1)
#             print(data_path)
#             print(data_ques)
        return data_ques

    elif role == "scheduler":
        msg_que = []
        msg_path = "/" + appName + "-msg" + "/" + str(stageId)
        msg_que = em.open_or_create_queue(msg_path, "local://tmp", 1)
        return msg_que

##################### update message also
# fin: its the last chunk
def write_jiffy_intermediate(table, data_queue, reduceId, fin):
    csv_buffer = BytesIO()
    if 'bin' in table.columns:
        slt_columns = table.columns.delete(table.columns.get_loc('bin'))
    else:
        slt_columns = table.columns
#     print(table)
   ####### table.to_csv(csv_buffer, sep="|", header=False, index=False, columns=slt_columns)
    encoded = table.to_csv(sep="|", header=False, index=False, columns=slt_columns).encode('utf-8')
#     print("before putting data to queue" + str(reduceId))
    data_queue[reduceId].put(encoded)
#     print("data size is" + str(sys.getsizeof(encoded)))
#     print("put data to queue" + str(reduceId))
#     ########## check if right
   # msg['datasize'][reduceId] = table[0].count
  #  print(table)
  #  print("size is" + str(table[0].count))
    shuffle_size = len(table)    #table[0].count
    #bucket_index = int(md5(output_loc).hexdigest()[8:], 16) % n_buckets
  #  s3_client.put_object(Bucket="hong-tpcds-" + str(bucket_index),
    if fin == 1:
        a = 'fin'
        data_queue[reduceId].put(a.encode('utf-8'))
    output_info = {}
  #  output_info['loc'] = output_loc
    output_info['names'] = slt_columns
    output_info['dtypes'] = table.dtypes[slt_columns]

    return output_info, shuffle_size


######## msg is a dict including per round message
############# write to all reducer queues, write to message queue

def write_jiffy_partitions(df, column_names, bintype, partitions, data_ques, msg_que, msg, fin):
    #print(df.columns)

    t0 = time.time()
    indices = [df.columns.get_loc(myterm) for myterm in column_names]
    #print(indices)
    bins = add_bin(df, indices, bintype, partitions)
    t1 = time.time()
    # print("t1 - t0 is " + str(t1-t0))
#     print((bins))
#     print(range(len(bins)))
#     print(df)

    outputs_info = []
############ bin_index is reduceId

    def write_task(bin_index):
        split = df[df['bin'] == bin_index]
        if split.size > 0 or split.size < 1:
            # print(split.size)
            split.drop('bin', axis=1, inplace=True)
            #print(split.dtypes)
            # write output to storage
        #    output_loc = storage + str(bin_index) + ".csv"
       #     print(split)

        #    outputs_info.append(write_jiffy_intermediate(split, data_ques, bin_index, fin, msg))
            out, shuffle_size = write_jiffy_intermediate(split, data_ques, bin_index, fin)
            outputs_info.append(out)
            msg['data_size'][bin_index] = shuffle_size
    write_pool = ThreadPool(1)
    write_pool.map(write_task, range(len(bins)-1))   ###########make sure it is right
  #  write_pool.map(write_task, range(4))
    write_pool.close()
    write_pool.join()
    t2 = time.time()

    results = {}
    results['outputs_info'] = outputs_info
    results['breakdown'] = [(t1-t0), (t2-t1)]

    ############
    #msg['roundId'] =
    msg['round_time'] = msg['round_time'] + (t2-t0)
    msg_que.put(json.dumps(msg))

    return results,msg


#def create_msg(roundId,total_round,round_time):
def create_msg(total_round,taskId,roundId,partition_num):
    msg = {}
    msg['total_round'] = total_round
    msg['roundId'] = roundId
    msg['taskId'] = taskId
    msg['round_time'] = 0
    msg['data_size'] = partition_num*[0]
 #   msg['fin'] = 0
 #   if roundId == total_round:
 #       msg['fin'] = 1
    return msg


################### read one data piece from the data_que with the reduceId
def read_jiffy_intermediate(key, reduceId, data_ques):


    try:

        obj = data_ques[reduceId].get()
#         print(sys.getsizeof(obj))
  #      print("the size of obj is"+ str(sys.getsizeof(obj)))
        ############ change here
        if sys.getsizeof(obj) < 64:
    #         part_data == 'fin'
    #         print("the fin is " + part_data)
#             print("here I am fin")
            return('fin')
        elif sys.getsizeof(obj) >1000:
    #             names = list(key['names'])
    #             dtypes = key['dtypes']
    #             parse_dates = []
    #             for d in dtypes:
    #                 if dtypes[d] == datetime.datetime or dtypes[d] == np.datetime64:
    #                     parse_dates.append(d)
    #                     dtypes[d] = np.dtype("string")
    #             part_data = pd.read_table(BytesIO(obj['Body'].read()),
    #                                       delimiter="|",
    #                                       header=None,
    #                                       names=names,
    #                                       dtype=dtypes,
    #                                       parse_dates=parse_dates)
          #  print("I am here")
       #     part_data = pd.read_table(BytesIO(obj),header = None, delimiter="|",  names = list(key['names']))
            part_data = pd.read_table(BytesIO(obj),header = None, delimiter="|",  names = ['key','value2'])
      #  b = pd.read_table(BytesIO(a),header = None, delimiter="|",  names = ['key','value','value2'])
#             print(part_data)
    except:                      ################## no data is in the queue right now
        part_data = 'emptyabcdddddddddddddddddddasdddddddddafsafasdfsdafsdfsdfdsfsdf'

    return part_data



###### a wraper which count fin number and support read in batch

def read_jiffy_splits(names, dtypes, reduceId, data_ques, fin_num, batch_size, fin_size):
    dtypes_dict = {}
    count = 0
#     for i in range(len(names)):
#         dtypes_dict[names[i]] = dtypes[i]
    lim = 0
    ds = pd. DataFrame()
    key = {}
    key['names'] = names
#     key['dtypes'] = dtypes_dict
    while count < batch_size and lim < 100:
        d = read_jiffy_intermediate(key, reduceId-1, data_ques)
#         print(type(d))
        print(sys.getsizeof(d))
        lim += 1
       # print("this is d: " + d)
        if sys.getsizeof(d) <70:
            fin_num += 1
            print("now what is fin_size?")
            print(fin_size)
            if fin_num == fin_size:
                break
        elif sys.getsizeof(d) > 1000:
            ds = ds.append(d)
#             print("ds is:")
#             print(ds)
            count += 1
        else:
            time.sleep(0.1)
    return ds, fin_num

###################### execute a stage ,tasks are keylists

def execute_lambda_stage(stage_function, tasks):
    t0 = time.time()
    #futures = wrenexec.map(stage_function, tasks)
    #pywren.wait(futures, 1, 64, 1)
    for task in tasks:
        task['write_output'] = True


    futures = wrenexec.map(stage_function, tasks)
#     futures = wrenexec.map_sync_with_rate_and_retries(stage_function, tasks, straggler=False, WAIT_DUR_SEC=5, rate=pywren_rate)


    results = [f.result() for f in futures]
    run_statuses = [f.run_status for f in futures]
    invoke_statuses = [f.invoke_status for f in futures]
    t1 = time.time()
    res = {'results' : results,
           't0' : t0,
           't1' : t1,
           'run_statuses' : run_statuses,
           'invoke_statuses' : invoke_statuses}
    return res


###################### update the status of a stage by reading message queue
def update_stage_status(msg_que,acc_size,round_per_map,time_per_map,total_round_per_map):
    try:
        msg = msg_que.get()
        msg = json.loads(msg)
        acc_size = acc_size + msg['data_size']
        round_per_map[msg['taskId']] = msg['roundId']
        time_per_map[msg['taskId']] += msg['round_time']
        total_round_per_map[msg['taskId']] = msg['total_round']
    except:
        a = 1
    return acc_size,round_per_map,time_per_map,total_round_per_map


#     msg['roundId'] = roundId
#     msg['taskId'] = taskId
#     msg['round_time'] = round_time
#     msg['data_size'] = partition_num*[0]


################# you need some function at the scheduler to close data and msg queue when a stage finishes
def close_stage_ques(em, stageId, appName):
    for reduceId in range(num_partitions):
        data_path = "/" + appName + "/" + str(stageId) +  str(reduceId)
  #      data_path = "/" + key['appName'] + "/" +  str(reduceId)
        em.close(data_path)
    msg_path = "/" + appName + "-msg" + "/" + str(stageId)
    em.close(msg_path)

########################## can I pass the func in?
############# launch the ith task in a stage (not s)
def launch_a_task(task_Id,launched_ones, unlaunched_ones,pro,func, key_list):
    pro.apply_async(pywren_sort_redis.sort_data, key_list)
   # pywren_sort_redis.sort_data(reduce_Id, 1, reducers, emnode, appName, shared_var)
    launched_ones.append({'reducer':task_Id,
                          'time':time.time()})
    unlaunched_ones.remove(task_Id)
    print("launched reducer " + str(task_Id) + " at time" + str(time.time()))
    return launched_ones, unlaunched_ones



em = '172.31.12.102'
appName = 'test-1'
############################### a map stage:
def toymap(key,share):
    def run_command(key):
        pywren.wrenlogging.default_config('INFO')
        logging.basicConfig(level = logging.DEBUG)
        logger = logging.getLogger(__name__)
        logger.info("before everything")
        logger.info(key)
        t_s = time.time()
        partition_num = key['partition_num']
        rounds = key['rounds']
        #em = key['em']
        taskId = key['taskId']
        appName = key['appName']
#         partition_num = 1
#         rounds = 8
#         #em = key['em']
#         taskId = 1
#         appName = 'test-1'
        em = JiffyClient(host=key['em'])
        logger.info("berfore queue")
        data_ques, msg_que = open_or_create_jiffy_queues(em, appName, partition_num, 1, 'sender')
        logger.info("queue opend")

        for i in range(rounds):
    ###        dd = read_s3_table(key)

            msg = create_msg(rounds, taskId, i, partition_num)

    #########  create a table here to replace the input
            right_table = 100000
            indices = tm.makeStringIndex(right_table).values
            key = np.tile(indices[:right_table], 1)
            right = DataFrame(
                {"key": key, "value": np.random.randn(right_table)}
            )
            logger.info("Finish generating data")
            x = 0
            if i == rounds - 1:
                x = 1
            encoded = right.to_csv(sep="|", header=False, index=False).encode('utf-8')
            a = np.random.randint(1,10,500000)
            encoded = np.asarray(a).astype('S100').tobytes()
           # print(sys.getsizeof(encoded))

            data_path = "/" + appName + "/" + '01'
            test_que = em.open_or_create_queue(data_path,"local://tmp", 10,1)
            logger.info("get encoded size" + str(sys.getsizeof(encoded)))
            ta= time.time()
            test_que.put(encoded)
            tb= time.time()
            logger.info("wirte takes" + str(tb-ta))
            logger.info("before get")
            obj = test_que.get()
            logger.info("get obj of size" + str(sys.getsizeof(obj)))
            tc= time.time()
            logger.info("get takes " + str(tc-tb))
#             data_ques[0].put(encoded)
#             logger.info("wirte finished")
#             logger.info("before get")
#             obj = data_ques[0].get()




            #res = write_jiffy_partitions(right, ['key'], 'uniform', partition_num, data_ques, msg_que = msg_que, msg = msg, fin = x)


        t_f = time.time()
#        share.append([t_s,t_f])

        return ([t_s,t_f])

    wrenexec = pywren.default_executor()
    #wrenexec = pywren.standalone_executor()
    keylist = []
    keylist.append(key)
    print(keylist)
    futures = wrenexec.map(run_command, keylist)
 #   for key in keylist:
 #       run_command(key)

    pywren.wait(futures)
    results = [f.result() for f in futures]
    share.append(results)


################# create task keys
# table = "date_dim"
# names = get_name_for_table(table)
# dtypes = get_dtypes_for_table(table)
# tasks_stage1 = []
# task_id = 0
# all_locs = get_locations(table)
# chunks = [all_locs[x:min(x+10,len(all_locs))] for x in xrange(0, len(all_locs), 10)]
# for loc in chunks:
#     key = {}
#     # print(task_id)
#     key['task_id'] = task_id
#     task_id += 1
#     key['loc'] = loc
#     key['names'] = names
#     key['dtypes'] = dtypes
#     key['output_address'] = temp_address + "intermediate/stage1"

#     tasks_stage1.append(key)
# tasks_stage1 = []
# task_id = 0
# for loc in chunks:
#     key = {}
#     # print(task_id)
#     key['task_id'] = task_id
#     task_id += 1
#     key['loc'] = loc
#     key['names'] = names
#     key['dtypes'] = dtypes

#     tasks_stage1.append(key)
######
# if '1' not in stage_info_load:
#     results_stage = execute_stage(stage1, tasks_stage1)
#     #results_stage = execute_local_stage(stage1, [tasks_stage1[0]])
#     stage1_info = [a['info'] for a in results_stage['results']]
#     #print(stage1_info)
#     stage_info_load['1'] = stage1_info[0]
#     #print("111")
#     #print(stage_info_load['1'])
#     #print("end111")
#     results.append(results_stage)
#     pickle.dump(results, open(filename, 'wb'))
#     pickle.dump(stage_info_load, open(stage_info_filename, 'wb'))

# results_stage = execute_stage(stage1, tasks_stage1)




##################### a reducer/join stage:
def toyjoin(key, share):
    def run_command(key):
        pywren.wrenlogging.default_config('INFO')
        logging.basicConfig(level = logging.DEBUG)
        logger = logging.getLogger(__name__)
        logger.info("before everything")
        partition_num = key['partition_num']
        rounds = key['rounds']
        em = JiffyClient(host=key['em'])
        reduceId = key['taskId']
        appName = key['appName']
        alg_type = key['type']
        data_ques1 = open_or_create_jiffy_queues(em, appName, partition_num, 1, 'receiver')
        logger.info("queue opened")
        names = key['names']
        dtypes = key['dtypes']
        ############# left table
        left_table = 100000
        indices = tm.makeStringIndex(left_table).values
        key = np.tile(indices[:left_table], 1)
        left = DataFrame(
            {"key": key, "value": np.random.randn(left_table)}
        )
        t_start = time.time()
        ############### initialize join functions
       # print(left)
        lim = 0
        ############## keeps fetching
        fin_num = 0

        if alg_type == 'pipelined':
            leftsorter = None
            leftcount = None
            orizer = None
            intrizer = None
            count = 0

            while fin_num<partition_num and lim <15:
                #### read table
                lim += 1
                time.sleep(0.01)
                logger.info("before get")
                obj = data_ques1[0].get()
                if sys.getsizeof(obj)>1000:
                    part_data = pd.read_table(BytesIO(obj),header = None, delimiter="|",  names = ['key','value2'])

            #    ds, fin_num = read_jiffy_splits(names, dtypes, reduceId, data_ques1, fin_num, batch_size = 1, fin_size = partition_num)
                logger.info(ds)
                logger.info(fin_num)
#                 print(fin_num)
                if len(ds)>0:
                ### join
    #                 start = timeit.default_timer()
                    result, orizer, intrizer, leftsorter, leftcount = pipeline_merge(left, ds, factorizer=orizer, intfactorizer=intrizer, leftsorter=leftsorter, leftcount=leftcount, slices=8, how="pipeline")
                    time.sleep(0.8)
                    logger.info("merged")
    #                 end = timeit.default_timer()
    #                 count += (end - start)
    #                 logger.info(str(i) + " chunks take time " +  str(end - start) + " Accum time: " + str(count))

        elif alg_type == 'origin':
            ds = pd. DataFrame()
            while fin_num<partition_num and lim <1500:
                lim += 1
                #### read table
                dd, fin_num = read_jiffy_splits(names, dtypes, reduceId, data_ques1, fin_num, batch_size = 1, fin_size = partition_num)
                if len(dd)>0:

                    ds = ds.append(dd)
                print("this is ds:")
                print(ds)
                result = merge(left, ds, how="inner")
                print(fin_num)
        t_fin = time.time()
#         share.append([t_start,t_fin, fin_num])
        return ([t_fin, t_start])
   # wrenexec = pywren.default_executor()
    wrenexec = pywren.standalone_executor()
    keylist = []
    keylist.append(key)
    print(keylist)
    futures = wrenexec.map(run_command, keylist)
 #   for key in keylist:
 #       run_command(key)

    pywren.wait(futures)
    results = [f.result() for f in futures]
    share.append(results)


#
#
#
def launch_a_stage(stageId, parent ,strategy):
    ##### check messages from parents
    ##### calculate based on algorithm the launch time of each task
    #####
    for i in range(taskId):
        if islaunched[i] == 0:
           # run algorithm
            if res == 1:
                launch_a_task(task_Id,launched_ones, unlaunched_ones,pro,func, key_list)

    partition_num = key['partition_num']
    rounds = key['rounds']
    em = key['em']
    taskId = key['taskId']
    appName = key['appName']


if __name__ == '__main__':


    key = {}
    # print(task_id)
    key['taskId'] = 1
    key['appName'] = 'test-1'
    #key['names'] = names
    # key['dtypes'] = dtypes
    key['partition_num'] = 1
    key['rounds'] = 4
    key['em'] = em

    key2 = {}
    # print(task_id)
    key2['taskId'] = 1
    #key['names'] = names
    # key['dtypes'] = dtypes
    key2['partition_num'] = 1
    key2['rounds'] = 1
    key2['em'] = em
    key2['names'] = []
    key2['dtypes'] = []
    # key2['type'] = 'origin'
    key2['type'] = 'pipelined'
    key2['appName'] = 'test-1'



    manager = Manager()


    shared_var_1 = manager.list([])
    shared_var_2 = manager.list([])


# p1 = Process(target = toymap, args = (key,shared_var_1,))
# p2 = Process(target = toyjoin , args = (key2,shared_var_2,))

# # os.system("~/jiffy/sbin/stop-all.sh")
# # os.system("~/jiffy/sbin/start-all.sh")

# p1.start()
# p2.start()
# p1.join()
# p2.join()

# print(shared_var_1)
# print(shared_var_2)
# print("done")


t_in = time.time()
toymap(key,shared_var_1)
t_mid = time.time()
print(shared_var_1)

# toyjoin(key2,shared_var_2)
# print(shared_var_2)

# t_out, t_start = toyjoin(key2,shared_var_2)
# mid = t_mid - t_in
# st = t_start - t_in
# dur = t_out - t_in
# print(mid, st, dur)

# print(shared_var_2)

# data_ques0, x = open_or_create_jiffy_queues(em, 'test-1', 2, 1, 'sender')
# right_table = 10000
# indices = tm.makeStringIndex(right_table).values
# key = np.tile(indices[:right_table], 1)
# right = DataFrame(
#     {"key": key, "value": np.random.randn(right_table)}
# )
# encoded = right.to_csv(sep="|", header=False, index=False).encode('utf-8')
# print("before putting data to queue" + str(0))
# # print(data_ques0[0])
# # print(data_ques0[1])
# data_ques0[0].put(encoded)
# emx = JiffyClient(host= em)

# data_path = "/" + appName + "/" + '01'
# test_que = emx.open_or_create_queue(data_path,"local://tmp", 10,1)
# obj = test_que.get()
# print(sys.getsizeof(obj))

# data_ques1 = open_or_create_jiffy_queues(emx, 'test-1', 1, 1, 'receiver')
# # print(data_ques1[0])
# obj = data_ques1[0].get()
# # # obj = data_ques1[0].get()
# print(sys.getsizeof(obj))
# # print("ok")
# # obj = data_ques1[0].get()
# part_data = pd.read_table(BytesIO(obj),header = None, delimiter="|",  names = ['key','value2'])
# print(part_data)
