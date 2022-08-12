# raft-playground

This project is a showcase of [Raft](https://raft.github.io/) algorithm.

Key entities in the codebase:
- `Config` contains basic algorithm settings
- `Cluster` is just a set of nodes
- `Node` represents a node in a cluster. 
  - Node has unique ID and internal `State`.
  - Nodes communicate with each other via network
- `Network` simulates a computer network.
  - Network may delay packet delivery
  - Network can lose or duplicate packets  
  - Network may have short-term partitions