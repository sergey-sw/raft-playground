# raft-playground

This project is a showcase of [Raft](https://raft.github.io/) algorithm.

Key entities in the codebase:
- `Config` contains basic algorithm settings
- `Cluster` is just a set of nodes
- `Node` represents a node in a cluster. 
  - Node has unique ID and internal state.
  - Nodes communicate with each other
- `Network` represents a computer network. It delivers messages between nodes.
  - Network may delay message delivery
  - Network can lose or duplicate messages  
  - Network may have a partition