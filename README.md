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

<hr/>

## 🧑‍🎓 How Raft works [simplified] 👩‍🎓

### Leadership

Node in a cluster can play one of three roles: follower, candidate, leader.
- At the beginning, all nodes are followers. From the name it may seem that the role of a follower implies the 
presence of the leader, but this is optional. Node can be a follower and listen to the leader, 
or it may be a follower and have no leader yet. If there's the leader in the cluster then a follower will receive updates from him. 
If a follower gets no updates from the leader within a `heartbeat timeout`, it will decide that the leader is down and promote 
itself to a candidate role.
- Candidate is a transitory role between the follower and the leader. A follower promotes itself to a candidate role if it does 
not hear from active leader or other candidates. When node becomes a candidate, it broadcasts a vote request to all other nodes. 
If the majority of the cluster (`n/2 + 1` nodes) responds OK to this vote request, a candidate will become the leader. 
Otherwise, node will step back to a follower role and the process will repeat after some delay.
- The leader sends heartbeats and updates to other nodes in a cluster. By design, there can be only one leader. 
If there are two or more leaders somehow, then you may be 100% sure that the implementation of raft algorithm is incorrect.

Election happens in a specific `term`. Each node starts in term `1`. Terms are incremented on new elections and help to 
logically order the events. [Lamport clock](https://en.wikipedia.org/wiki/Lamport_timestamp) is a close analogue 
to terms in Raft. Election may either finish with the new chosen leader or finish without the leader (no one got 
the majority of the votes) — in this case there will be new election in a higher term.

### Data updates

All updates are handled by the leader. If a client sends an update to the node that is not the leader, 
this node should respond with error or redirect.

The leader maintains an ordered log of data operations and replicates it to the followers. The leader also maintains 
the indices of the last replicated operation on each follower. When the leader receives an update, it appends the operation to its log and 
broadcasts it to the followers. If the majority of the cluster responds OK to this update, the leader applies this update 
and responds OK to the client.

To ensure the correctness of the operations order in the operations log, messages sent from the leader to the followers 
contain not only the operation itself, but also the index and term of the last committed operation on the leader. 
Follower appends the update only if its operations log matches leader's log (last operation is same). 
Follower may have an outdated operations log due to different reasons. 
In that case a follower replies with error and sends the index of the last operation in its log.
The leader will understand that this follower missed some previous updates and will send them since the provided index.

The durability of the update is guaranteed by the nature of voting: since the majority of the cluster confirmed the 
acceptance of the update, it's guaranteed that every further majority will contain a node that has this update.

As stated earlier, the leader applies the update to its local state right after it receives the confirmation from the 
majority of the cluster. Followers, on the other hand, are always one step behind: they need to receive one more 
update from the leader to ensure that the previous one was successfully committed to the log. Because of that, clients 
should read data only from the leader if they need strict data consistency.

<hr/>

## Leader election demo

Let's run `LeaderElectionDemo` and see what happens step by step.

We start the cluster with the following settings:

```kotlin
val clusterConfig = ClusterConfig(
    nodeCount = 5,
    electionTimeoutMinMs = Time.millis(150),
    electionTimeoutMaxMs = Time.millis(300),
    heartbeatTimeoutMs = Time.millis(3_000)
)
```

- There are 5 nodes in a cluster. 
- Nodes have random election delay between `150` and `300` milliseconds.
This is the amount of time a node will wait until it promotes itself as a candidate. 
This timer restarts if the node receives a vote request from another candidate.
- Leader heartbeat timeout is `3` seconds.
If a node does not hear from the leader for `3` seconds, it will promote itself as a candidate.

We use these network settings:

```kotlin
val networkConfig = NetworkConfig(
    messageDeliveryDelayMillis = 5,
    messageLossProbability = 0.0f,
    messageDuplicationProbability = 0.0f,
    partitionsEnabled = true
)
```

- Messages are delivered within `5` milliseconds. They can not be lost or duplicated (but you may change this setting). 
- Network partitions are enabled to make the example more dynamic. Otherwise, once the leader is chosen it will hold 
leadership infinitely.

Then we create and add some `VotingNode` to cluster and start it:

```kotlin
val cluster = Cluster<VotingNode>(clusterConfig, networkConfig)
for (i in 1..clusterConfig.nodeCount) {
    cluster.add(
        VotingNode(NodeID("n$i"), clusterConfig, cluster.network)
    )
}

cluster.start()
```

Before we go further, let's review a `VotingNode` class. 

`VotingNode` implements the `Node` interface.
It has unique `nodeID` property for identification and `start` method for startup.
Also, there are two important `process` methods. 
The first one is invoked when a candidate requests a vote from this node. 
The second one is invoked when this node receives a heartbeat from the leader.

```kotlin
interface Node {
    val nodeID: NodeID
    
    fun start()
    fun process(req: VoteRequest): VoteResponse
    fun process(req: AppendEntries): AppendEntriesResponse
}
```

On startup, you will see some debug statements that show cluster and network settings:

```
2022-10-31 00:19:35.736 raft-cluster INFO Logging to /tmp/raft-log-2022-10-31-00-19
2022-10-31 00:19:35.754 raft-cluster INFO Starting raft cluster
2022-10-31 00:19:35.756 raft-cluster INFO Nodes: [n5, n1, n3, n4, n2]
2022-10-31 00:19:35.756 raft-cluster INFO Network message delay millis: 5
2022-10-31 00:19:35.756 raft-cluster INFO Network message loss probability: 0.0
2022-10-31 00:19:35.757 raft-cluster INFO Network message duplication probability: 0.0
2022-10-31 00:19:35.757 raft-cluster INFO Raft election delay millis: 150..300
2022-10-31 00:19:35.757 raft-cluster INFO Raft heartbeat timeout millis: 3000
```

Then you will see that all nodes are started. 

Each node will sleep for `election timeout` milliseconds before trying to become the leader.
Waiting interval is random within predefined bounds, so some nodes will wake up earlier than the others.
```
2022-11-19 18:09:44.559 raft-node-n5 INFO Node n5 started
2022-11-19 18:09:44.559 raft-node-n5 INFO Will wait 196 ms before promoting self to candidate
2022-11-19 18:09:44.561 raft-node-n1 INFO Node n1 started
2022-11-19 18:09:44.562 raft-node-n1 INFO Will wait 221 ms before promoting self to candidate
2022-11-19 18:09:44.562 raft-node-n3 INFO Node n3 started
2022-11-19 18:09:44.562 raft-node-n3 INFO Will wait 285 ms before promoting self to candidate
2022-11-19 18:09:44.562 raft-node-n4 INFO Node n4 started
2022-11-19 18:09:44.563 raft-node-n4 INFO Will wait 228 ms before promoting self to candidate
2022-11-19 18:09:44.563 raft-node-n2 INFO Node n2 started
2022-11-19 18:09:44.563 raft-node-n2 INFO Will wait 282 ms before promoting self to candidate```
```

In our example `node-5` election timeout was `196 ms`, so it woke up first and promoted itself as a candidate.
This election is made in term `1`. Terms monotonically increase during the lifetime of the cluster. 
This is done to logically order the events, since we can't just rely on the clock time.

```
2022-11-19 18:09:44.731 raft-node-n5 INFO Became a candidate in term 1 and requested votes from others
```

Since all other nodes slept for at least `30ms` longer than 'node-5', there is no competition. 
So it's easy for `node-5` to win this election.

Here we see how other nodes vote in response to the VoteRequest from 'node-5'. They also refresh the election timeout 
after voting. This is needed to preserve some time for the current election to finish normally.

```
2022-11-19 18:09:44.732 raft-node-n2 INFO Voted for n5 in term 1
2022-11-19 18:09:44.732 raft-node-n2 INFO Will wait 298 ms before promoting self to candidate
2022-11-19 18:09:44.732 raft-node-n3 INFO Voted for n5 in term 1
2022-11-19 18:09:44.732 raft-node-n3 INFO Will wait 216 ms before promoting self to candidate
2022-11-19 18:09:44.733 raft-node-n1 INFO Voted for n5 in term 1
2022-11-19 18:09:44.733 raft-node-n1 INFO Will wait 290 ms before promoting self to candidate
2022-11-19 18:09:44.734 raft-node-n4 INFO Voted for n5 in term 1
2022-11-19 18:09:44.734 raft-node-n4 INFO Will wait 203 ms before promoting self to candidate
```

Receiving first two votes is enough for a candidate to become a new leader: the majority for a `5` node cluster is `3`, 
and `1` vote is already reserved, because a candidate voted for itself.

```
2022-11-19 18:09:44.734 raft-node-n5 INFO Accepting VoteResponse{granted=true} in term 1 from follower n1
2022-11-19 18:09:44.734 raft-node-n5 INFO Node is still candidate in term 1, followers: [n5, n1]
2022-11-19 18:09:44.737 raft-node-n5 INFO Accepting VoteResponse{granted=true} in term 1 from follower n4
2022-11-19 18:09:44.737 raft-node-n5 INFO Node n5 became leader in term 1 with followers: [n1, n4, n5]
```

At this point of time other nodes do not yet know that there is a new leader in a cluster.
Leader should broadcast a heartbeat to notify everyone that election succeeded.


```
2022-11-19 18:09:44.737 raft-node-n5 INFO Sent leader heartbeat in term 1. Follower delays: {n1=6, n4=4}
```

Meanwhile, leader `node-5` continues to receive vote responses to its initial vote request from the rest of the cluster.
These nodes are added to the follower list.

```
2022-11-19 18:09:44.741 raft-node-n5 INFO Received VoteResponse for term 1 from n2, add to followers: [n1, n2, n4, n5]
2022-11-19 18:09:44.741 raft-node-n5 INFO Received VoteResponse for term 1 from n3, add to followers: [n1, n2, n3, n4, n5]
```

When nodes receive a heartbeat from the leader, they start following it and await for commands.

```
2022-11-19 18:09:44.741 raft-node-n4 INFO Node n4 received AppendEntries(leader=n5) and accepted leadership of node n5 in term 1
2022-11-19 18:09:44.746 raft-node-n3 INFO Node n3 received AppendEntries(leader=n5) and accepted leadership of node n5 in term 1
2022-11-19 18:09:44.747 raft-node-n1 INFO Node n1 received AppendEntries(leader=n5) and accepted leadership of node n5 in term 1
2022-11-19 18:09:44.747 raft-node-n2 INFO Node n2 received AppendEntries(leader=n5) and accepted leadership of node n5 in term 1
```

In this demo we have no data operations on the cluster, so pretty nothing happens after the leader is chosen. You may 
have noticed that heartbeats are sent every `750` ms. That's because the heartbeat timeout is `3` seconds. The leader is 
configured to send `4` heartbeats within a timeout interval. The number corresponding to the nodeID (follower delays) 
is the time elapsed since the last follower's response.

```
2022-11-19 18:09:45.027 raft-node-n5 INFO Sent leader heartbeat in term 1. Follower delays: {n1=281, n2=278, n3=281, n4=283}
2022-11-19 18:09:45.776 raft-node-n5 INFO Sent leader heartbeat in term 1. Follower delays: {n1=748, n2=744, n3=744, n4=746}
2022-11-19 18:09:46.527 raft-node-n5 INFO Sent leader heartbeat in term 1. Follower delays: {n1=745, n2=744, n3=742, n4=746}
```

### Network partitions

The network was configured to have random partitions. They eventually occur:

```
2022-11-19 18:09:49.971 network WARNING >> Network partition happened: [[n1], [n2], [n3], [n4, n5]] <<
```

Nodes `n1`, `n2` and `n3` got isolated and can't reach any other node in the cluster. 
Nodes `n4` and `n5` can communicate with each other.
Because of the partitions, leader's (`node-5`) heartbeats can't reach any node except `node-4`.

```
2022-11-19 18:09:50.276 raft-node-n5 INFO Sent leader heartbeat in term 1. Follower delays: {n1=745, n2=746, n3=744, n4=742}
2022-11-19 18:09:51.026 raft-node-n5 INFO Sent leader heartbeat in term 1. Follower delays: {n1=1495, n2=1496, n3=1494, n4=746}
2022-11-19 18:09:51.776 raft-node-n5 INFO Sent leader heartbeat in term 1. Follower delays: {n1=2245, n2=2246, n3=2244, n4=746}
2022-11-19 18:09:52.526 raft-node-n5 INFO Sent leader heartbeat in term 1. Follower delays: {n1=2995, n2=2996, n3=2994, n4=742}
```

Eventually nodes `n1`, `n2` and `n3` decide, that leader is down, so they try to promote themselves.
Because of the partition, these nodes can not reach any other node in the cluster. Their promotion attempts will always fail:

```
2022-11-19 18:09:52.746 raft-node-n3 INFO Became a candidate in term 2 and requested votes from others
2022-11-19 18:09:52.821 raft-node-n1 INFO Became a candidate in term 2 and requested votes from others
2022-11-19 18:09:52.826 raft-node-n2 INFO Became a candidate in term 2 and requested votes from others
2022-11-19 18:09:53.046 raft-node-n3 INFO Didn't get enough votes, step down to FOLLOWER at term 2
2022-11-19 18:09:53.121 raft-node-n1 INFO Didn't get enough votes, step down to FOLLOWER at term 2
2022-11-19 18:09:53.126 raft-node-n2 INFO Didn't get enough votes, step down to FOLLOWER at term 2
```

They will try again and again. You may notice that each new attempt increments the election term.

```
2022-11-19 18:09:54.836 raft-node-n3 INFO Became a candidate in term 6 and requested votes from others
2022-11-19 18:09:55.021 raft-node-n1 INFO Became a candidate in term 6 and requested votes from others
2022-11-19 18:09:55.021 raft-node-n2 INFO Became a candidate in term 6 and requested votes from others
2022-11-19 18:09:55.136 raft-node-n3 INFO Didn't get enough votes, step down to FOLLOWER at term 6
2022-11-19 18:09:55.321 raft-node-n1 INFO Didn't get enough votes, step down to FOLLOWER at term 6
2022-11-19 18:09:55.321 raft-node-n2 INFO Didn't get enough votes, step down to FOLLOWER at term 6
```

Meanwhile, nodes `n4` and `n5` stay in the first term, because they did not see new elections:

```
2022-11-19 18:09:55.526 raft-node-n5 INFO Sent leader heartbeat in term 1. Follower delays: {n1=5995, n2=5996, n3=5994, n4=746}
```

Finally, the network partition is resolved and nodes need to agree on who is the new `leader`.
There's a rule that a node should step down to the `follower` state if it sees a term higher that its own term.
Nodes `n4` and `n5` are in term `1`, while other nodes got ahead in their unsuccessful election attempts. 
After they talk to each other, nodes `n4` and `n5` will understand they got behind and need to update their terms.

In our example, `n2` was first to promote itself to a `candidate` after the network partition was resolved, 
so it has good chances to become the new `leader`:

```
2022-11-19 18:09:59.596 raft-node-n2 INFO Became a candidate in term 15 and requested votes from others
2022-11-19 18:09:59.601 raft-node-n4 INFO Voted for n2 in term 15
2022-11-19 18:09:59.601 raft-node-n2 INFO Accepting VoteResponse{granted=true} in term 15 from follower n4
2022-11-19 18:09:59.601 raft-node-n2 INFO Node is still candidate in term 15, followers: [n2, n4]
2022-11-19 18:09:59.601 raft-node-n2 INFO Received VoteResponse{granted=false} for term 15 from n3 in term 15. Current role is CANDIDATE
```

As you may see, when current leader node `n5` gets a message from `n2` in term 15, it steps down and votes for `n2`:

```
2022-11-19 18:09:59.601 raft-node-n5 INFO Stepping down from LEADER role in term 1: received vote request for term 15 from n2
2022-11-19 18:09:59.601 raft-node-n5 INFO Voted for n2 in term 15
2022-11-19 18:09:59.601 raft-node-n5 INFO Will wait 150 ms before promoting self to candidate
```

Finally, node `n2` becomes the new `leader`:

```
2022-11-19 18:09:59.606 raft-node-n2 INFO Accepting VoteResponse{granted=true} in term 15 from follower n5
2022-11-19 18:09:59.606 raft-node-n2 INFO Node n2 became leader in term 15 with followers: [n2, n4, n5]
2022-11-19 18:09:59.606 raft-node-n2 INFO Sent leader heartbeat in term 15. Follower delays: {n4=2, n5=0}
```

### Summary of the leader election demo

In this simplified example you learned the basics of the leader election process in Raft. Let's review it:

Nodes promote themselves from `follower` to `candidate` role and try to win the election.

Each election happens in a specific `term`. Terms start from `1` and increase monotonically in each new round of election. 
Terms represent the order of events or in some sense, time.

Only one node can win the election in the given term, because 
- winning requires the majority of the cluster to vote for the same node, and 
- node can not vote several times in a single term. 

Once the leader is elected, it starts broadcasting heartbeats to all other nodes in the cluster to hold its leadership. 

Network partitions can isolate some nodes from the leader. If a node does not hear from the leader longer 
than a `heartbeat timeout`, it decides that leader is down and tries to become the new leader. 

New election happens in a greater term than the previous election. After the network partition is resolved, nodes need to 
agree on the new leader. RPC requests and responses contain the info about the current term of the node. 
When node sees a term `X` higher than its own term `T`, it understands that it got behind and steps down to a follower 
role in term `X`. Eventually nodes will synchronize on the same term and agree on the new leader.

#### Disclaimer

This was a simplified example without any data operations in the cluster. That's why nodes with higher term values 
can easily win the election. Full version of the algorithm adds another requirement for the voting process: 
a node will decline the voting request if the candidate's operations log is behind the node's operation log. 
See more details in the log replication demo.

<hr/>

## Log replication demo

To be continued