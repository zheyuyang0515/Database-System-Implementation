# Database-System-Implementation
Implementing database management system similar to MySQL, Oracle.  
The project will support the following functionalities: query processing and optimization. transaction processing, index structures, parallel query processing.  

- Week1 - Week2: Implemented DBFile class. The job of the DBFile class within your database system is simply to store and retrieve records from the disk.  

- Week3 - Week5: Implemented BigQ class. The BigQ class encapsulates the process of taking a stream of inserts, breaking the stream of inserts into runs, and then using an in-memory priority queue to organize the head of each run and give the records to the caller in sorted order.  

- Week6 - Week8: Extended the DBFile class so that it implements both a sorted file and a heap. I did most of the work for the heap file in the last assignment, so the vast majority of the work involved implementing the sorted variation of the DBFile class which could store the records in a given sorted order


