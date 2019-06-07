## CMPS128
# Fault Tolerant Scalable Key-Value Store

  - Docker is used to simulate a multi-machine implementation
  
  - hw4_test.py represents a unit test for the latest version.
  
  - Leverages gossip communication protocol to encourage eventtual consistency among replicas.
  
  - Partitions data across nodes to allow storage of more key-value pairs than could fit on one machine (in one container).
  
  - Message forwarding makes distribution partition is transparent to client.
  
  
# Running Tests

  - Supposing that you have docker installed:
  
  - Launch Docker
  
  - `python3 hw4_tests.py`. This will take a few minutes to run, and doesn't do much apart from informing you that nothing bad happened.
