package main

import (
	"os/exec"
)

var POOL_NAME = "storage"

func CheckPoolExistance() bool {
	cmd := exec.Command("zpool", "list", POOL_NAME)
	output, err := cmd.Output()

	if err != nil {
		panic(err)
	}
	outputStr := string(output)

	if len(outputStr) > 0 {
		return true
	} else {
		return false
	}
}

func CreatePool() {
	if !CheckPoolExistance() {
		cmd := exec.Command("zpool", "create", POOL_NAME, "/dev/vdb")
		err := cmd.Run()
		if err != nil {
			panic(err)
		}
	}
}

func CreateSnapshot(number string) {
	cmd := exec.Command("zfs", "snapshot", POOL_NAME+"@"+number)
	err := cmd.Run()
	if err != nil {
		panic(err)
	}
}

func SendSnapshotIncremental(sender string, receiver string, beg string, end string) {
	cmd := exec.Command("sudo", "zfs", "send", "-i", POOL_NAME+"@"+beg, POOL_NAME+"@"+end, "ssh", receiver, "zfs", "recv", POOL_NAME)
	err := cmd.Run()
	if err != nil {
		panic(err)
	}
}
