# Raft consensus 🤝 written in Kotlin 👨🏻‍💻

This project is a showcase of [Raft](https://raft.github.io/) distributed consensus algorithm.

You can run `LeaderElection` and `LogReplication` demo scenarios from `src/xyz/skywind/demo` folder to see how it works.

This implementation can not be used in production systems, since nodes are just application threads,
state is not persistent and communication does not use real network.

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

#### Key files in the codebase:
- `ClusterConfig` contains  basic algorithm settings
- `Cluster` is just a set of nodes
- `Node` represents a node in a cluster.
    - `Node` has unique `NodeID` and internal `State`.
    - `VotingNode` implements leader election.
    - `DataNode` extends `VotingNode` and implements `ClientAPI` (data operations)
        - `Data` is a key-value storage and operations log
        - `OperationsLog` stores all the mutation operations (e.g. set/remove) performed on the cluster
    - `TimerTask` helps to run periodical tasks, such as leader heartbeats or candidate promotions.
- Nodes communicate with each other via `Network`
- `Network` simulates a computer network. Configured via `NetworkConfig`.
    - It may delay packet delivery
    - It can lose or duplicate packets
    - It may have short-term partitions
- `xyz/skywind/raft/rpc` package contains RPC messages