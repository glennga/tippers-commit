# tippers-commit
Development of 2PC for use with the TIPPERS benchmark.

## Setup
1. First and foremost, verify that you have the following: PostgreSQL and Anaconda.

2. Next, we want to create a Python virtual environment for testing. Run the following commands:
    ```bash
    > cd tippers-commit
    > conda env create -f environment.yml
    > conda activate tippers-commit-env
    ```
    
3. Create a fresh PostgreSQL instance on each node that you plan to run the TM daemon on. Modify the `config/postgres.json` to include your credentials. Ensure that the user you provide is a super-user. For simplicity, we assume that all nodes have the same credentials. 

4. Ensure that each PostgreSQL instance has `max_prepared_transactions` set to a reasonable value (we used 50). This allows us to use Postgres's 2PC interface.

5. Execute the `create.sql` file and the `metadata.sql` file on all instances.

6. Modify the `config/site.json` file to include all nodes that can be involved in a transaction. This includes the coordinator node. If desired, modify the `config/manager.json` file to change how the TM daemon operates.

7. You are now ready to run the TM daemon! To view the transaction generator + manager in action, run the TM daemon on all nodes (`python3 manager.py <site alias>`) and run the transaction generator on any one of the given nodes (`python3 generator.py`).
