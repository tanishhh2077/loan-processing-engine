Question 1: Exploring the SQL Database and HDFS file size (5 points)

A. How In Part 1, you were asked to use an AI assistant to find the necessary SQL commands to explore the CS544 database. Using your own words (that is don't copy/paste AI output), summarize the AI's response. List the commands the AI suggested and what you discovered using them, such as the table names and the relevant columns for the join operation.(2 points)

Aider suggested the following SQL commands:

SHOW TABLES; - Revealed two tables: hdma_2021 (loan application data) and institutions_2021 (lender information)
DESCRIBE hdma_2021; and DESCRIBE institutions_2021; - Showed the column structures
SELECT * FROM hdma_2021 LIMIT 5; - Examined sample data to understand the schema

Aider dentified that:

Both tables share the lei column, which could be useful for an inner join. It also identified that I needed to filter 30000 < loan_amount < 800000

B. List the HDFS command that AI suggested for exploring the file size. (1 point)

hdfs dfs -du -h hdfs://nn:9000/

C. Using your own words (that is don't copy/paste AI output), summarize the AI's response for your question on why the file size might be different on HDFS. List any additional prompts (do not copy/paste the output for the prompts) that you had to use to get the AI assistant to talk about compression related details.

Prompts I used:

"Why might the HDFS file size differ from expectations?"
"Can you explain how compression affects Parquet file sizes?"

Aider explained that file size differences occur primarily due to Parquet's columnar compression. Parquet uses compression codecs (typically Snappy by default) that reduce file size based on data patterns.

Question 2: Understanding Performance (5 points)

In Part 3B, you were instructed to ask an AI to explain why partitioned data queries are faster. Summarize the key points from the AI's response.

The AI explained that partitioning the large hdma-wi-2021.parquet file into county-specific partition files significantly improves performance for repetitive reads due to reduced I/O operations, smaller file sizes, and eliminating redundant filtering. 

Question 3: Analyzing NameNode Logs (10 points)

In Part 4, after killing a DataNode, you were required to collect the NameNode logs and use an AI to analyze them. Based on your interaction with the AI, answer the following:
A. How does the NameNode detect a DataNode failure? (2 points)

The NameNode uses a heartbeat mechanism. DataNodes send periodic signals to the NameNode every 3 seconds. If the NameNode doesn't receive heartbeats within the configured timeout period, it marks the DataNode as "dead" and assumes it has failed.

B. What specific log messages indicate that blocks are now under-replicated?(2 points)

INFO org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager: Marking Datanode <datanode_id> as dead
INFO org.apache.hadoop.hdfs.server.blockmanagement.BlockManager: Number of under replicated blocks: <count>
WARN org.apache.hadoop.hdfs.server.blockmanagement.BlockManager: Block <block_id> on <datanode> has replication <current> but needs <required>

C. How long did HDFS take to officially mark the DataNode as dead?(3 points)

10 minutes and 30 seconds 

D. What recovery mechanisms did HDFS attempt automatically?(3 points)

HDFS checks whether each block that lived on the failed DataNode through NameNode is now under-replicated. If yes, it scheduled for this block to be replaced. New replicas are placed based on replacement policies such as data locality and fault tolerance. 
