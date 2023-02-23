## Design
The `ptor` tool designed to validate the `RPO`, `RTO` values
after we do the failover/switchover of the postgresql instances.

This tool also helpful to evaluate the cloud providers `RPO`, `RTO` and `SLA`.

To use `ptor` tool, we need below two instances.

1. `PRIMARY_PGDSN` which points to the primary endpoint
2. `REPO_PGDSN` which makes a copy of the messages, which parallel workers execute on the `PRIMARY_PGDSN`.
We need this `REPO_PGDSN` instance, to validate the data loss `RPO` after we perform the switchover/failover.

![](./ptor.png)
## Quick Test

### Local
Quick test performed between from the two local instance, where primary and repo instances are running local. The `ptor` tool is also running local.

Here, we restarted the local `primary` instance manually to mimic the `failover/switchover`.

[![asciicast](https://asciinema.org/a/2MRLVcmL2cm7V4eWCtM9rj0Yf.svg)](https://asciinema.org/a/2MRLVcmL2cm7V4eWCtM9rj0Yf)


## Demo
All the demos are done with a sync replica among the nodes.
Also, all the nodes are in the same network to avoid any latencies.



[Ptor pg_auto_failover](https://youtu.be/_0vhXn0HbWU)

[Ptor Stolon](https://youtu.be/SDriI00HNbM)

[Ptor Patroni, HaProxy](https://youtu.be/NoDMljx8_Q0)



## Installation

Below are the installation steps, which are prepared on RHEL instance. If you are using debain flavour, then use the platform specific package tools like `apt-get` or `brew` to install the below components.


1. Install `git`

        $ sudo yum install git -y

2. Install `golang`

        $ sudo yum install golang -y
        

3. Install `PostgreSQL server` (Optional)

        $ sudo yum install postgresql-server -y

    This is for the repo server, where we save a copy of primary transactions.

4. Download the copy of `ptor` source

        $ git clone git@github.com:dineshkumar02/ptor.git

5. Build the `ptor` binary

        $ cd ptor
        $ make

## Usage
| Option                 | Usage                                                                                                                     |
|------------------------|---------------------------------------------------------------------------------------------------------------------------|
| --repo-pgdsn           | The `repo` PostgreSQL connection string, where it syncs primary data.                                                     |
| --primary-pgdsn        | The primary or service PostgreSQL connection string, where we run switchover/failover.                                    |
| --parallel-workers     | Number of parallel workers to run data loading. It will create these many individual tables.                              |
| --init                 | Initialize the `paralle-workers` tables.                                                                                  |
| --reset                | Delete all data from `repo` and `primary` instances.                                                                      |
| --warmup-duration      | Initial data loading duration in seconds.                                                                                 |
| --insert-percent       | Percentage number of insert operations.                                                                                   |
| --update-percent       | Percentage number of update operations.                                                                                   |
| --delete-percent       | Percentage number of delete operations.                                                                                   |
| --full-data-validation | Run full data validation on both `repo` and `primary` in the end of the test case.                                        |
| --async-repo-mode      | All `primary` events will get in sync to `repo` asynchronously. This improves more data generation on the `primary` side. |
| --rto-conn-timeout           | `Primary` dns connection timeout value. This helps in calculating the `RTO` |
| --check-primary-latency      | Check network connectivity latency between `ptor` and  `primary` dns |


