""" This script will generate and send TIPPERS transactions to a TM daemon. """

import argparse
import json

from shared import *


class _TransactionGenerator(GenericSocketUser):
    def __init__(self, **context):
        super().__init__()
        self.context = context

    def _insert_statement(self, transaction_id: bytes, statement: str, hash_input: list) -> bool:
        """ Send the insertion to TM. Additionally, send as input the object to hash on (in our case, this is the
        <sensor_id, timestamp>).

        :param transaction_id: ID associated with the current transaction.
        :param statement: INSERT statement to perform.
        :param hash_input: Input to hash our nodes on. This determines where to redirect the request to.
        :return: True if the insertion succeeded. False otherwise.
        """
        logging.debug(f"Sending INSERT to the transaction manager for transaction {transaction_id}.")
        logging.debug(f"Sending statement: {statement}")
        self.send_message(OpCode.INSERT_FROM_CLIENT, [transaction_id, statement, hash_input])

        reply_message = self.read_message()
        logging.debug("Received from transaction manager: ", reply_message)
        return reply_message == ResponseCode.OK

    def _start_transaction(self) -> bytes:
        """ :return: The transaction ID. """
        self.send_op(OpCode.START_TRANSACTION)
        transaction_id = self.read_message()
        logging.info(f"Starting transaction. Issued ID: ", str(transaction_id))
        return transaction_id

    def _abort_transaction(self, transaction_id: bytes):
        """ Abort all transactions up until this point. """
        logging.info(f"Sending ABORT message to the transaction manager for transaction {transaction_id}.")
        self.send_message(OpCode.ABORT_TRANSACTION, [transaction_id])
        logging.info("Received from transaction manager: ", self.read_message())

    def _commit_transaction(self, transaction_id: bytes):
        """ Commit all transactions up until this point. """
        logging.info(f"Sending COMMIT message to the transaction manager for transaction {transaction_id}.")
        self.send_message(OpCode.COMMIT_TRANSACTION, [transaction_id])
        logging.info("Received from transaction manager: ", self.read_message())



    @staticmethod
    def convert_str_timestamp(timestamp):

        timestamp = timestamp.replace(" ","")
        timestamp = timestamp.replace("'","")
        timestamp = timestamp[0:10] + " " + timestamp[10:]
        timestamp = dt.strptime(timestamp, "%Y-%m-%d %H:%M:%S")

        return timestamp
    

    def _process_transaction(self, insert_list: list):
        tr_id = self._start_transaction()
        #print(insert_list)
        for insert in insert_list:
            flag = self._insert_statement(tr_id, insert[0], insert[1])
            if flag == False:
                self._abort_transaction(tr_id)
                break

        self._commit_transaction(tr_id)



    def __call__(self):
        hostname, port = self.context['coordinator_hostname'], int(self.context['port'])
        logging.info(f"Connecting to TM at {hostname} through port {port}.")
        self.socket.connect((hostname, port))

        # Complete this portion-- note: we are doing hashing at the TM instead of the client now...
        filename = self.kwargs['benchmark-file']
        time_delta = self.kwargs['time_delta']


        file_r = open(filename, "r")

        sensor_dict = {}

        cur_timestamp = convert_str_timestamp(file_r.readline().rstrip().split(",")[-2])
        cur_timestamp = cur_timestamp + datetime.timedelta(0,time_delta)

        file_r.seek(0)

        #line = ""
        ctr = 0
        error = []


        while True:
            try:
        
                record = file_r.readline().rstrip()
                if record == "":
                    break
                    continue
                
                #error = [record]
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
                        process_transaction(v)
                    
                    cur_timestamp = cur_timestamp + datetime.timedelta(0,time_delta)
                    sensor_dict = {}

                ctr += 1
                
            except Exception as e:
                print(e)
                print(sys.exc_info()[-1].tb_lineno)
                break

        file_r.close()



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate and submit TIPPERS benchmark transactions.')
    parser.add_argument('--config_path', type=str, default='config', help="Location of the configuration files.")
    c_args = parser.parse_args()

    with open(c_args.config_path + 'generator.json') as generator_config_file:
        generator_json = json.load(generator_config_file)

    _TransactionGenerator(
        coordinator_hostname=generator_json['coordinator-hostname'],
        coordinator_port=generator_json['coordinator-port'],
        benchmark_file=generator_json['benchmark-file'],
        time_delta=generator_json['time-delta']
    )()
