# tippers-commit
Development of 2PC for use with the TIPPERS benchmark.

## Setup
1. First and foremost, verify that you have the following: PostgreSQL and Anaconda.

2. Next, we want to create a Python virtual environment for testing. Run the following commands:
    ```bash
    > cd tippers-commit
    > conda env create -f environment.yml
    > conda activate tippers-benchmark-env
    ```

3. Download the _reorganized_ project files. From the project source, these have been sorted and modified to work with our system.

    TODO: INSERT THE FILE LINKS HERE.

    If necessary, modify the `config/generator.json` file to point to the correct project files.
    
4. Create a fresh PostgreSQL instance on each node that you plan to run the TM daemon on. Modify the `config/postgres.json` to include your credentials. Ensure that the user you provide is a super-user. For simplicity, we assume that all nodes have the same credentials. 

5. Modify the `config/site.json` file to include all nodes that can be involved in a transaction. This includes the coordinator node. If desired, modify the `config/manager.json` file to change how the TM daemon operates.

6. You are now ready to run the TM daemon!

## Testing
All tests were performed on a 9-node Raspberry Pi cluster. The testing scripts are specific to the (custom) Akala cluster, but we describe the intent of each script below. All scripts can be found in `test`.

- I/O is significantly more expensive here (USB 2.0 rates vs. typical HDD rates), which places greater emphasis on "lazy persistence". We do not want to needlessly flush our logs here.
- The purpose of these tests are to validate the correctness of our 2PC implementation, at the participant site and the coordinator site. 
- For brevity, these tests only work with one transaction. To run the full TIPPERS insert-only workload, modify the `config/generator.json` file to specify a larger workload file.
- _Failures_ here refer to a when a process stops beyond the specified timeout periods, and continues as planned. We are able to inject failures through the use of modified Coordinator and Participant processes. These are set to fail at certain points of 2PC, allowing us to observe the behavior of other "normal" processes.

#### Test Case #1: No Failures (Happy Path)
The first test case consists of a happy path: no process failures. All processes launched are not-modified from our implementation of 2PC.

#### Test Case #2: Participant Reboots Before Receiving PREPARE


#### Test Case #3: Participant Reboots After Receiving PREPARE, Before Sending ACK

#### Test Case #4: Participant Reboots After Receiving PREPARE, Before Receiving COMMIT

#### Test Case #5: Participant Reboots After Receiving PREPARE, Before Receiving ABORT

#### Test Case #6: Coordinator Reboots Before Sending PREPARE

#### Test Case #7: Coordinator Reboots After Sending Partial PREPARE

#### Test Case #8: Coordinator Reboots After Sending PREPARE, Before Receiving Responses

#### Test Case #9: Coordinator Reboots After Sending PREPARE, After Receiving Responses

#### Test Case #10: Coordinator Reboots After Sending PREPARE, After Receiving Partial Responses

#### Test Case #11: Coordinator Reboots After Sending Partial COMMIT