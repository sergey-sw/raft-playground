# raft-playground

This project is a showcase of [Raft](https://raft.github.io/) algorithm.

Key entities in the codebase:
- `Config` contains all essential algorithm settings
- `Cluster` is just a set of nodes
- `Node` represents a node in a cluster. 
  - Node has unique ID and internal state.
  - Nodes communicate with each other
  - Nodes communication is implemented with messaging, not with RPC. It better fits the network representation.
- `Network` represents a computer network. It delivers messages between nodes.
  - Network may delay message delivery
  - Network may have a partition
- `Message` and its subclasses represent different types of Raft commands.