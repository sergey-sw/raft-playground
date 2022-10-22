# raft-playground

This project is a showcase of [Raft](https://raft.github.io/) distributed consensus algorithm.

Check `leader election` and `log replication` demo scenarios in ``xyz/skywind/demo`` folder. 

<hr/>

#### Supported functional requirements:
- ✅ Leader election
- ✅ Log replication
- ❌ Cluster membership updates

#### Supported non-functional requirements:
- ❌ Persistent storage for state and data
- ❌ Log snapshots
- ❌ Operations pipelining

<hr/>

#### Key entities in the codebase:
- `ClusterConfig` contains some basic algorithm settings
- `Cluster` is just a set of nodes
- `Node` represents a node in a cluster.
    - `Node` has unique `NodeID` and internal `State`.
    - `VotingNode` only supports leader election.
    - `DataNode` extends `VotingNode` and implements `ClientAPI` (data operations)
      - `Data` is a container for the key-value storage and operations log
      - `OperationsLog` stores all the mutation operations (e.g. set/remove) performed on the cluster
    - `TimerTask` helps to run periodical tasks, such as leader heartbeats or candidate promotions.
- Nodes communicate with each other via `Network`
- `Network` simulates a computer network. Configured via `NetworkConfig`.
  - It may delay packet delivery
  - It can lose or duplicate packets  
  - It may have short-term partitions
- `xyz/skywind/raft/rpc` package contains RPC messages