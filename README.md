# raft-playground

This project is a showcase of [Raft](https://raft.github.io/) algorithm.

Key entities in the codebase:
- `Config` contains basic algorithm settings
- `Cluster` is just a set of nodes
- `Node` represents a node in a cluster. 
  - Implementation is hierarchical:
    - `VotingNode` contains code to support leader election
    - `DataNode` extends `VotingNode` and implements `ClientAPI` (data operations)
  - Node has unique `NodeID` and internal `State`.
  - Nodes communicate with each other via `Network`
- `Network` simulates a computer network.
  - It may delay packet delivery
  - It can lose or duplicate packets  
  - It may have short-term partitions