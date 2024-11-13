# Project Requirements

## Distributed Replicated Concurrency Control and Recovery

Iplement a distributed database, complete with 
1. serializable snapshot isolation
2. replication
3. failure recovery

### Data
20 Distinct variables, x1 ... x20.

10 Distinct sites from 1 to 10. 

x6.2 is variable x6 at site 2.

All odd indexed variables are only copied to sites corresponding to their last digit

- x1, x11 are at site 2
- x2, x12 are at site 3
- x3, x13 are at site 4 etc 

Even indexed variables are present at all sites (full replication)

### Replication
#### Available copies approach
- Read from any copy
- Write to all copies

On read - if client transaction cannot read from a site, read from another server

On write, if client cannot write to x, write to all other copies provided there is one.

If a site is unavailable, don't need to write to it.
Likewise for reads, if we only have a site, read from the only site available.




### Serializable Snapshot Isolation
Recall that Snapshot Isolation works as follows:

(1) Reads from transaction Ti
read committed data as of the time Ti began.

(2) Writes follow the first committer wins rule:
Ti will successfully commit only if no other concurrent transaction 
Tk has already committed writes to data items where Ti has written
versions that it intends to commit.

That is, if (a) Ti starts at time start(Ti) and tries to commit at end(Ti);
(b) Tk commits between start(Ti) and  end(Ti);
and (c) Tk writes some data item x that Ti wants to write, then Ti should abort.

We need to abort transactions with R-W cycles.

Maintain Dependency Graph of transactions.

Remove transactions which have completed.

2 phase commit? 


### Design

#### Transaction Manager
1. Singleton - Translates read and write requests on variables to read and write requests on copies using available copies algorithm.
2. Also acts as a broker/load balancer
3. Transaction manager will hold in-flight transactions

#### Data Managers
1. Represesnt the sites.

Idea here
We represent entire system as a single threaded system (or actors)

Interface is a transaction manager
- Keeps track of ongoing transactions, their current state, as well as some kind of transaction dependency graph.
- Snapshot Isolation implementation -> Every site keeps a history of all previous values
    - We keep track of the earliest timestamp transaction
    - Trim values in each site whenever transacitions end. Use List to maintain transactions
    - Maintain running (global) timestamp for each action
- Each DM is simply a dictionary
- Waiting transactions are rolled over into the next time period.
- Transaction will hold state as the 'result' of the transaction
    - Dict mapping of new values to be committed at each site.
    - Writes are stored within ongoing transaction.
    - Waiting transactions are held with operations pending until some site comes back up (Transactions will only wait when trying to read a variable where all sites are down.)
- 




#### Test Cases 
1. begin(T1) -> Transaction starts
2. R(T1, x6) -> Read x6 from any available site -> Print value on newline
3. W(T1, x2, v) -> Write v to all available copies of x2 -> Prints nothing 
4. end(T1) -> Completes T1, report if T1 can commits or aborts
5. dump -> dumps all values from committed sites
6. fail(6) -> Fails a site
7. recover(7) -> Recovers a site


