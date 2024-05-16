// Copyright 2017,2018 Lei Ni (nilei81@gmail.com).
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/*
helloworld is an example program for dragonboat.
*/
package main

import (
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"github.com/garlik6/raft-zfs/zfs"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/logger"
	"github.com/lni/goutils/syncutil"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	exampleShardID uint64 = 128
)

var ( // initial nodes count is fixed to three, their addresses are also fixed these are the initial member nodes of the Raft cluster.
	addresses = []string{
		"10.128.0.15:63000",
		"10.128.0.26:63000",
		"10.128.0.10:63000",
	}
	errNotMembershipChange = errors.New("not a membership change request")
)

// makeMembershipChange makes membership change request.
func makeMembershipChange(nh *dragonboat.NodeHost,
	cmd string, addr string, replicaID uint64) {
	var rs *dragonboat.RequestState
	var err error
	if cmd == "add" {
		// orderID is ignored in standalone mode
		rs, err = nh.RequestAddReplica(exampleShardID, replicaID, addr, 0, 3*time.Second)
	} else if cmd == "remove" {
		rs, err = nh.RequestDeleteReplica(exampleShardID, replicaID, 0, 3*time.Second)
	} else {
		panic("unknown cmd")
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "membership change failed, %v\n", err)
		return
	}
	select {
	case r := <-rs.CompletedC:
		if r.Completed() {
			fmt.Fprintf(os.Stdout, "membership change completed successfully\n")
		} else {
			fmt.Fprintf(os.Stderr, "membership change failed\n")
		}
	}
}

// splitMembershipChangeCmd tries to parse the input string as membership change
// request. ADD node request has the following expected format -
// add localhost:63100 4
// REMOVE node request has the following expected format -
// remove 4
func splitMembershipChangeCmd(v string) (string, string, uint64, error) {
	parts := strings.Split(v, " ")
	if len(parts) == 2 || len(parts) == 3 {
		cmd := strings.ToLower(strings.TrimSpace(parts[0]))
		if cmd != "add" && cmd != "remove" {
			return "", "", 0, errNotMembershipChange
		}
		addr := ""
		var replicaIDStr string
		var replicaID uint64
		var err error
		if cmd == "add" {
			addr = strings.TrimSpace(parts[1])
			replicaIDStr = strings.TrimSpace(parts[2])
		} else {
			replicaIDStr = strings.TrimSpace(parts[1])
		}
		if replicaID, err = strconv.ParseUint(replicaIDStr, 10, 64); err != nil {
			return "", "", 0, errNotMembershipChange
		}
		return cmd, addr, replicaID, nil
	}
	return "", "", 0, errNotMembershipChange
}

func main() {
	replicaID := flag.Int("replicaid", 1, "ReplicaID to use")
	addr := flag.String("addr", "", "Nodehost address")
	join := flag.Bool("join", false, "Joining a new node")
	flag.Parse()
	if len(*addr) == 0 && *replicaID != 1 && *replicaID != 2 && *replicaID != 3 {
		fmt.Fprintf(os.Stderr, "node id must be 1, 2 or 3 when address is not specified\n")
		os.Exit(1)
	}
	// https://github.com/golang/go/issues/17393
	if runtime.GOOS == "darwin" {
		signal.Ignore(syscall.Signal(0xd))
	}
	initialMembers := make(map[uint64]string)
	// when joining a new node which is not an initial members, the initialMembers
	// map should be empty.
	// when restarting a node that is not a member of the initial nodes, you can
	// leave the initialMembers to be empty. we still populate the initialMembers
	// here for simplicity.
	if !*join {
		for idx, v := range addresses {
			// key is the ReplicaID, ReplicaID is not allowed to be 0
			// value is the raft address
			initialMembers[uint64(idx+1)] = v
		}
	}
	var nodeAddr string
	// for simplicity, in this example program, addresses of all those 3 initial
	// raft members are hard coded. when address is not specified on the command
	// line, we assume the node being launched is an initial raft member.
	if len(*addr) != 0 {
		nodeAddr = *addr
	} else {
		nodeAddr = initialMembers[uint64(*replicaID)]
	}
	fmt.Fprintf(os.Stdout, "node address: %s\n", nodeAddr)
	// change the log verbosity
	logger.GetLogger("raft").SetLevel(logger.ERROR)
	logger.GetLogger("rsm").SetLevel(logger.WARNING)
	logger.GetLogger("transport").SetLevel(logger.WARNING)
	logger.GetLogger("grpc").SetLevel(logger.WARNING)
	// config for raft node
	// See GoDoc for all available options
	rc := config.Config{
		// ShardID and ReplicaID of the raft node
		ReplicaID: uint64(*replicaID),
		ShardID:   exampleShardID,
		// In this example, we assume the end-to-end round trip time (RTT) between
		// NodeHost instances (on different machines, VMs or containers) are 200
		// millisecond, it is set in the RTTMillisecond field of the
		// config.NodeHostConfig instance below.
		// ElectionRTT is set to 10 in this example, it determines that the node
		// should start an election if there is no heartbeat from the leader for
		// 10 * RTT time intervals.
		ElectionRTT: 10,
		// HeartbeatRTT is set to 1 in this example, it determines that when the
		// node is a leader, it should broadcast heartbeat messages to its followers
		// every such 1 * RTT time interval.
		HeartbeatRTT: 1,
		CheckQuorum:  true,
		// SnapshotEntries determines how often should we take a snapshot of the
		// replicated state machine, it is set to 10 her which means a snapshot
		// will be captured for every 10 applied proposals (writes).
		// In your real world application, it should be set to much higher values
		// You need to determine a suitable value based on how much space you are
		// willing use on Raft Logs, how fast can you capture a snapshot of your
		// replicated state machine, how often such snapshot is going to be used
		// etc.
		SnapshotEntries: 10,
		// Once a snapshot is captured and saved, how many Raft entries already
		// covered by the new snapshot should be kept. This is useful when some
		// followers are just a little bit left behind, with such overhead Raft
		// entries, the leaders can send them regular entries rather than the full
		// snapshot image.
		CompactionOverhead: 5,
	}
	datadir := filepath.Join(
		"example-data",
		"helloworld-data",
		fmt.Sprintf("node%d", *replicaID))
	// config for the nodehost
	// See GoDoc for all available options
	// by default, insecure transport is used, you can choose to use Mutual TLS
	// Authentication to authenticate both servers and clients. To use Mutual
	// TLS Authentication, set the MutualTLS field in NodeHostConfig to true, set
	// the CAFile, CertFile and KeyFile fields to point to the path of your CA
	// file, certificate and key files.
	nhc := config.NodeHostConfig{
		// WALDir is the directory to store the WAL of all Raft Logs. It is
		// recommended to use Enterprise SSDs with good fsync() performance
		// to get the best performance. A few SSDs we tested or known to work very
		// well
		// Recommended SATA SSDs -
		// Intel S3700, Intel S3710, Micron 500DC
		// Other SATA enterprise class SSDs with power loss protection
		// Recommended NVME SSDs -
		// Most enterprise NVME currently available on the market.
		// SSD to avoid - Consumer class SSDs, no matter whether they are SATA or NVME based, as
		// they usually have very poor fsync() performance.
		//
		// You can use the pg_test_fsync tool shipped with PostgreSQL to test the
		// fsync performance of your WAL disk. It is recommended to use SSDs with
		// fsync latency of well below 1 millisecond.
		//
		// Note that this is only for storing the WAL of Raft Logs, it is size is
		// usually pretty small, 64GB per NodeHost is usually more than enough.
		//
		// If you just have one disk in your system, just set WALDir and NodeHostDir
		// to the same location.
		WALDir: datadir,
		// NodeHostDir is where everything else is stored.
		NodeHostDir: datadir,
		// RTTMillisecond is the average round trip time between NodeHosts (usually
		// on two machines/vms), it is in millisecond. Such RTT includes the
		// processing delays caused by NodeHosts, not just the network delay between
		// two NodeHost instances.
		RTTMillisecond: 200,
		// RaftAddress is used to identify the NodeHost instance
		RaftAddress: nodeAddr,
	}
	nh, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		panic(err)
	}
	if err := nh.StartReplica(initialMembers, *join, NewExampleStateMachine, rc); err != nil {
		fmt.Fprintf(os.Stderr, "failed to add cluster, %v\n", err)
		os.Exit(1)
	}
	raftStopper := syncutil.NewStopper()
	ch := make(chan string, 16)
	raftStopper.RunWorker(func() {
		ticker := time.NewTicker(10 * time.Second)
		for {
			select {
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				result, err := nh.SyncRead(ctx, exampleShardID, []byte{})
				cancel()
				if err == nil {
					var snap uint64
					snap = binary.LittleEndian.Uint64(result.([]byte))
					fmt.Fprintf(os.Stdout, "last snapshot in the system: %d\n", snap)
					ch <- strconv.FormatInt(int64(snap+1), 10)
				}
			case <-raftStopper.ShouldStop():
				return
			}
		}
	})

	raftStopper.RunWorker(func() {
		http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
			leader_id, _, has_leader, err := nh.GetLeaderID(exampleShardID)
			if err != nil {
				panic(err)
			}
			var status int
			var message string
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			membership, err := nh.SyncGetShardMembership(ctx, exampleShardID)
			if err != nil {
				log.Print("error while getting Membership")
				status = 503
				message = "I dont have a leader"
				w.WriteHeader(status)
				fmt.Fprint(w, message)
				cancel()
				return
			}
			nodes := membership.Nodes
			cancel()

			if !has_leader {
				status = 503
				message = "I dont have a leader"
				w.WriteHeader(status)
				fmt.Fprint(w, message)
				return
			}

			if isCurrentNodeLeader(nodes, leader_id) {
				status = 200
				message = "I am leader"
				w.WriteHeader(status)
				fmt.Fprint(w, message)
				return
			}

			if !isCurrentNodeLeader(nodes, leader_id) {
				status = 503
				message = "I am follower"
				w.WriteHeader(status)
				fmt.Fprint(w, message)
				return
			}
		})
		http.ListenAndServe(":9999", nil)
	})

	raftStopper.RunWorker(func() {
		ticker := time.NewTicker(10 * time.Second)
		for {
			select {
			case <-ticker.C:
				leader_id, _, has_leader, err := nh.GetLeaderID(exampleShardID)
				if !has_leader {
					log.Print("cant contact any nodes! remounting to ro")
					zfs.SetRO()
					break
				}
				if err != nil {
					log.Print("error while getting leaderID")
					panic(err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				membership, err := nh.SyncGetShardMembership(ctx, exampleShardID)
				if err != nil {
					log.Print("error while getting Membership")
					cancel()
					break
				}
				nodes := membership.Nodes
				cancel()
				// send snapshot to slaves -> make with errorGroup propose to dragon boat only if at least 2 are succsessful
				if isCurrentNodeLeader(nodes, leader_id) {
					log.Print("I am leader")
					mountStatus, err := zfs.CheckMountStatus()
					log.Print(mountStatus)
					if err != nil {
						log.Print(err)
						panic(err)
					}
					if mountStatus != "RW" {
						log.Print("remounting rw")
						zfs.SetRW()
					}
				} else {
					log.Print("I am follower")
					mountStatus, err := zfs.CheckMountStatus()
					log.Print(mountStatus)
					if err != nil {
						log.Print(err)
						panic(err)
					}
					if mountStatus != "RO" {
						log.Print("remounting ro")
						zfs.SetRO()
					}
				}
			case <-raftStopper.ShouldStop():
				return
			}
		}
	})

	raftStopper.RunWorker(func() {
		// use a NO-OP client session here
		// check the example in godoc to see how to use a regular client session
		cs := nh.GetNoOPSession(exampleShardID)
		for {
			select {
			case newSnap, ok := <-ch: // from channel we get the number of last created snapshot

				logs := make(chan string)
				if !ok {
					return
				}
				newSnap = strings.Replace(newSnap, "\n", "", 1)
				leader_id, _, _, err := nh.GetLeaderID(exampleShardID)
				if err != nil {
					panic(err)
				}
				// get list of nodes
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				membership, err := nh.SyncGetShardMembership(ctx, exampleShardID)
				nodes := membership.Nodes

				// send snapshot to slaves -> make with errorGroup propose to dragon boat only if at least 2 are succsessful
				if isCurrentNodeLeader(nodes, leader_id) {
					println("I am leader")
					// if error creating snapshot retry and exit gracefully
					err = zfs.CreateSnapshot(newSnap, logs)
					if err != nil {
						log.Print("Failed to create snapshot exiting...")
						log.Fatal(err)
					}
					err = sendUpToSnapFromLeaderToFolowers(leader_id, newSnap, nodes)
					if err != nil {
						log.Fatal(err)
					}
					_, err = nh.SyncPropose(ctx, cs, []byte(newSnap))
					cancel()
					if err != nil {
						fmt.Fprintf(os.Stderr, "SyncPropose returned error %v\n", err)
						err = zfs.DestroySnapshot(newSnap)
						if err != nil {
							fmt.Fprintf(os.Stderr, "Failed to destroy snapshot\n", err)
							log.Fatal(err)
						}
					}
				}
				cancel()
			case <-raftStopper.ShouldStop():
				return
			}
		}
	})
	raftStopper.Wait()
}

func isCurrentNodeLeader(nodes map[uint64]string, leader_id uint64) bool {
	return strings.Contains(nodes[leader_id], getCurrentIp(nodes[leader_id]))
}

func getCurrentIp(leaderconn string) string {
	conn, err := net.Dial("tcp", leaderconn)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.TCPAddr)

	return localAddr.IP.String()
}

func sendUpToSnapFromLeaderToFolowers(leader_id uint64, newSnap string, nodes map[uint64]string) error {
	successfulCount := 0
	for node_id := range nodes {
		log.Print(strconv.Itoa(int(node_id)) + " " + nodes[node_id])
		if node_id != leader_id {
			ip := nodes[node_id][:strings.IndexByte(nodes[node_id], ':')]
			lastSnapOnNode, err := zfs.GetLastSnapNumber(ip)
			if err != nil {
				log.Print("unable to get last snap on " + ip)
				break
			}
			log.Print("trying to send to:")
			log.Print(nodes[node_id])
			if lastSnapOnNode == "-1" {
				err := zfs.SendSnapshotFull(ip, newSnap)
				if err != nil {
					log.Print("unable to send full snapshot")
					break
				}
				successfulCount++
			} else {
				log.Print("on " + ip + "last snap on this node is: " + lastSnapOnNode)
				err = zfs.SendSnapshotIncremental(ip, lastSnapOnNode, newSnap)
				if err != nil {
					log.Print("unable to send snapshot incremental")
					log.Print("sending snapshot full")
					err := zfs.SendSnapshotFull(ip, newSnap)
					if err != nil {
						log.Print("unable to send full snapshot")
						break
					}
					break
				}
				successfulCount++
			}
		}
	}
	if successfulCount == 0 {
		return fmt.Errorf("was not able to send snapshot to all slaves")
	}
	return nil
}
