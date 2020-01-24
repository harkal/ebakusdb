# EbakusDB

Each smart contract in ebakus has its own schema defined database (ESDD). This database can support any number of tables with typed fields and indexes. A smart contract is able to perform the following operations on the data:

Create/Drop tables
Create/Drop indexes on specific fields
Retrieve/update/delete single or multiple rows of data
Do ordered range queries on these data
The ebakus software makes sure that the data are stored in such a way in order to support the above operations in the most efficient way. The smart contract should not need to implement most common query types by itself.

The EbakusDB layer is providing to the ebakus blockchain a very fast database layer that supports O(1) time and space complexity snapshots. This is essential to the operation of a blockchain system that has requirements for querying old block states. The database achieves high performance by being aware of the transactional log functionality that the layer above it is using and not reimplementing it itself. Therefore achieving ACID compliance without sacrificing performance.

Smart contracts deployed in Ethereum compatibility mode will not be able to make use of the ESDD, hence will not be able to benefit from the extra functionality and performance.