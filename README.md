# Spark Streaming K-mer Counting Project  
 
This project demonstrates the use of **Apache Spark Streaming** to process real-time data streams and count the occurrences of k-mers (subsequences of length `k`) in text data. The project implements a **k-mer counting program** using PySpark and Spark Streaming to process continuous input data streamed from a TCP socket. Specifically, the program processes a generated text file (`sentences.txt`) and streams data through the TCP port `9999` to count 3-mers (k-mers of length 3).

