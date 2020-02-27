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
    for insert in insert_list:
        flag = insert_statement(tr_id, insert[0], insert[1])
        if flag == 0:
            abort_transaction(tr_id)
            break

    commit_transaction(tr_id)

    


file_r = open(filename, "r")

sensor_dict = {}

cur_timestamp = convert_str_timestamp(file_r.readline().rstrip().split(",")[-2])
time_delta = cur_timestamp + datetime.timedelta(0,120)
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
        #print(record)
        error = [record]
        timestamp = convert_str_timestamp(record.split(",")[-2])
        #error = [timestamp, record]

        sensor_id = record.split(",")
        sensor_id = sensor_id[0].split("(")
        sensor_id = sensor_id[1]
        sensor_id = sensor_id.replace("'","")

        if timestamp <= cur_timestamp:
            if sensor_id in sensor_dict:
                sensor_dict[sensor_id].append([[record],[sensor_id, timestamp]])


            else:
                sensor_dict[sensor_id] = [[[record], [sensor_id]]]

        else:
            print(sensor_dict)
            print("heis")

            for (k,v) in sensor_dict.items():
                process_transaction(v)
            #process_transaction(sensor_dict)
            
            cur_timestamp = cur_timestamp + datetime.timedelta(0,300)
            sensor_dict = {}

        ctr += 1
    except Exception as e:
        #process_transaction(sensor_dict)
        print(e)
        print(sys.exc_info()[-1].tb_lineno)
        print(error)
        break


print(sensor_dict)

file_r.close()



