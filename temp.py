from datetime import datetime as dt
import datetime
import sys


filename = "xaa.txt"

def start_transaction():
    return 5

def insert_statement(tr_id, a, b):
    return 1

def commit_transaction(tr_id):
    return 1




def convert_str_timestamp(timestamp):

    timestamp = timestamp.replace(" ","")
    timestamp = timestamp.replace("'","")

    timestamp = timestamp[0:10] + " " + timestamp[10:]

    timestamp = dt.strptime(timestamp, "%Y-%m-%d %H:%M:%S")



    return timestamp


def process_transaction(insert_list):
    tr_id = start_transaction()
    #print(insert_list)
    for insert in insert_list:
        flag = insert_statement(tr_id, insert[0], insert[1])
        if flag == False:
            abort_transaction(tr_id)
            break

    commit_transaction(tr_id)

    


file_r = open(filename, "r")

sensor_dict = {}

cur_timestamp = convert_str_timestamp(file_r.readline().rstrip().split(",")[-2])

cur_timestamp = cur_timestamp + datetime.timedelta(0,600)

#cur_timestamp = time_delta

#print(cur_timestamp, time_delta)
file_r.seek(0)

line = ""
ctr = 0

error = []
while True:
    try:
        
        record = file_r.readline().rstrip()
        if record == "":
            break
            continue
        
        error = [record]
        timestamp = convert_str_timestamp(record.split(",")[-2])
        
        sensor_id = record.split(",")
        sensor_id = sensor_id[-1]
        
        sensor_id = sensor_id.replace(")", "")
        sensor_id = sensor_id.replace(";", "")
        sensor_id = sensor_id.replace("'","")
        sensor_id = sensor_id.replace(" ", "")
        
        if timestamp <=  cur_timestamp:
            if sensor_id in sensor_dict:
                sensor_dict[sensor_id].append([[record],[sensor_id, timestamp]])


            else:
                sensor_dict[sensor_id] = [[[record], [sensor_id, timestamp]]]

        else:
            for (k,v) in sensor_dict.items():
                #print(len(v))
                process_transaction(v)
            #process_transaction(sensor_dict)
            
            cur_timestamp = cur_timestamp + datetime.timedelta(0,600)
            sensor_dict = {}

        ctr += 1
    except Exception as e:
        print(e)
        print(sys.exc_info()[-1].tb_lineno)
        print(error)
        break


print(sensor_dict)

file_r.close()



