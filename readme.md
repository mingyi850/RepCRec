# Replicated Concurrency Control and Recovery (RepCRec)

## Summary
This project aims to implement a simple K-V distributed database with serializable snapshot isolation, replication and failure recovery using available copies.

## Running the Project
The project is written in golang.

### Running the project directly
To run the project, ensure you have golang installed

1. Build the binary
    ``` 
    make
    ```
	Ensure that you have permission to run the binary
	```
	chmod +x repcrec
	```
2. Run in console mode (reads instructions from stdin)
    ```
    ./repcrec
    ```
3. Run in file mode (reads instructions from file)
    ```
    ./repcrec <inputfile>
    ```

## Running the project using [reprounzip](https://github.com/VIDA-NYU/reprozip)
Reprozip is a packaging tool which ensures portability across environments. Reprounzip is the counterpart which unpacks packages packaged by Reprozip and allows them to be run in any environment.

The ```repro-repcrec.rpz``` file was packaged on NYU Courant's CIMS cluster.

In both methods of running the project, the project is started in interactive mode. In order to provide  input from a file, please follow the instructions below

### Running on Linux using Reprozip
Running the project on a Linux machine is the simplest way. No additional dependencies are required.

1. Install reprounzip
	``` 
	pip install reprounzip 
	```
2. Unpack reprozip package
	```
	reprounzip directory setup repro-repcrec.rpz <directory_name>
	```
3. Run project in interactive mode
	```
	reprounzip directory run <directory_name>
	```
	This should start the database in interactive mode

4. To run the project with an provided input file, use 
	```
	cat <input_file> | reprounzip docker run <directory_name>
	```

### Running on Windows or OSx using Docker
To run the project on Windows or OSx, the most straightforward way will be to use docker.
Ensure that the docker daemon is running before using `reprounzip-docker` commands

1. Install reprounzip and reprounzip-docker
	```
	pip install reprounzip reprounzip-docker
	```
2. Unpack reprozip package - This creates as docker container which will be used to run the project
	```
	reprounzip docker setup repro-repcrec.rpz <directory_name>
	```
3. Run the project in interactive mode - This spins up a docker container
	```
	reprounzip docker run <directory_name>
	```

4. Run the project using a file
	```
	(cat <input_file> ; echo "exit") | reprounzip docker run <directory_name>
	```
	The `exit` command is neccesary to stop the simulation in interactive mode. Since running the project in reprozip automatically starts it in interactive mode, this step is neccesary
	



The program will output each line from the input, followed by the outcome of the operation. If any error is encountered during the parasing of the file or the operation of the program, it will terminate with the specified error.


## Design
The high level design of the database is as follows:

![Entity diagram for distributed database](image-1.png)

### Transaction Manager
The transaction manager acts as the interface with the database. Contains the transaction details of active transactions, a transaction graph for cycle detection and contains the main logic for executing transactions.

A rough overview of the class is as follows:

```
type TransactionManager interface {
	Begin(tx int, time int) error
	End(tx int, time int) (CommitResult, error)
	Write(tx int, key int, value int, time int) (WriteResult, error)
	Read(tx int, key int, time int) (ReadResult, error)
	Recover(site int, time int) error
	GetTransaction(tx int) (*Transaction, bool, error)
}

def Begin(transaction int, time int) -> Adds a transaction to the transaction pool

def End(transaction: Tx, time int) -> checks for RW cycles, write conflicts and site failures and tries to commit transaction if possible. Removes transaction from transaction_graph and map once done committed or aborted.

def Read(transaction: Tx, key: int, time int) -> Retrieves available sites for reads and attempts to read from any valid site, or waits if there is possible site which is currently down. Returns result if successful. Might abort transaction immediately if no sites are viable. 

def Write(transaction: Tx, key: int, value: int, time int) -> Attempts to write to all replicas of a site. Waits if no replicas are available to be written to.

def Recover(site: int) -> starts executing operations on transactions waiting for specific site

def GetTransaction(tx int) -> Gets a transaction, whether it's waiting and error if an error occurs
```

### Transaction
Each transaction stores its start time, status completed operations and pending operations in case it is waiting for a site to be made available.

The rough model of a transaction is as follows:
```
type Transaction struct {
	id                  int
	startTime           int
	siteWrites          map[int]Operation
	pendingOperations   []Operation
	completedOperations map[int][]Operation
	waitingSites        map[int]bool
	state               TransactionState
}
```

### TransactionGraph
The transaction Graph is represented as a directed graph, with nodes represented by the transaction id and edges added to the graph as values of these nodes.

type TransactionGraph struct {
	graph map[int]map[int]ConflictType
}

When a transaction completes (end command is issued) we check the transaction graph for potential conflicts. If we obtain a RW-RW cycle in the transaction graph, the transaction is aborted.

During this time, we also recursively purge the transaction graph of any outdated transactions which committed before the earliest start time, and are not part of any other dependencies. 

When a transaction is successfully committed, we add all dependencies to the transaction graph.

### Site Coordinator
The site coordinator keeps track of the uptime and history of each site, as well as it's current status. It also helps to retrieve relevant sites for the transaction manager.

The Site coordinator is also used for sending Up and down signals to the sites

```
type SiteCoordinator interface {
	Fail(site int, time int) error
	Recover(site int, time int) error
	Dump() string
	ReadActiveSite(site int, key int, time int) (HistoricalValue, error)
	GetSitesForKey(key int) []int
	GetActiveSitesForKey(key int) []int
	GetValidSitesForRead(key int, txStart int) []int
	VerifySiteWrite(site int, key int, writeTime int, currentTime int) SiteCommitResult
	CommitSiteWrite(site int, key int, value int, time int) error
}
```

### Site/DataManager
Sites are simply abstract representations of the data managers.
The DataManager at each site keeps track of the values held within the site, as well as the commit history of each value.
It also holds pending writes to values for each transaction (as volatile writes)
```
type DataManager interface {
	Dump() string
	Read(key int, time int) HistoricalValue
	Commit(key int, value int, time int) error
	GetLastCommitted(key int) HistoricalValue
}
```

We provide more detailed information about each component and it's methods in the code.


## Testing 
We provide unit tests in the ```/test``` folder.
To run unit tests, execute 
``` 
make test 
```
This will run the unit tests linked to the test scripts in the ```test/resources``` folder

The ```test/resources``` folder contains a list of test cases and descriptions about what should happen in those cases.





