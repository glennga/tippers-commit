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

3. Download the _reorganized_ workload file. From the project source, the high-concurrency workload has been sorted and modified to work with our system.

    TODO: INSERT THE FILE LINKS HERE.

    If necessary, modify the `config/generator.json` file to point to the correct project files.
    
4. Create a fresh PostgreSQL instance on each node that you plan to run the TM daemon on. Modify the `config/postgres.json` to include your credentials. Ensure that the user you provide is a super-user. For simplicity, we assume that all nodes have the same credentials. 

5. Ensure that each PostgreSQL instance has `max_prepared_transactions` set to a reasonable value (we used 50). This allows us to use Postgres's 2PC interface.

6. Modify the `config/site.json` file to include all nodes that can be involved in a transaction. This includes the coordinator node. If desired, modify the `config/manager.json` file to change how the TM daemon operates.

7. You are now ready to run the TM daemon!
