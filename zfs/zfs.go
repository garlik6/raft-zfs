package zfs

import (
	"bufio"
	"fmt"
	"log"
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

func CreatePool() error {
	if !CheckPoolExistance() {
		cmd := exec.Command("zpool", "create", POOL_NAME, "/dev/vdb")
		err := cmd.Run()
		if err != nil {
			log.Print(err)
			return err
		}
		return nil
	}
}

func CreateSnapshot(number string) error {
	cmd := exec.Command("zfs", "snapshot", POOL_NAME+"@"+number)
	err := cmd.Run()
	if err != nil {
		log.Print(err)
		return err
	}
	return nil
}

func SendSnapshotIncremental(receiver string, beg string, end string) error {
	cmd := exec.Command("bash", "-c", "zfs send -i "+POOL_NAME+"@"+beg+" "+POOL_NAME+"@"+end+" | ssh "+receiver+" zfs recv "+POOL_NAME)
	log.Print("send from " + beg + " to " + end + " (receiver" + receiver + ")")
	log.Print(cmd.String())
	stderr, _ := cmd.StderrPipe()
	err := cmd.Start()
	if err != nil {
		log.Print(err)
		return err
	}
	scanner := bufio.NewScanner(stderr)
	for scanner.Scan() {
		fmt.Println(scanner.Text())
	}
	return nil
}

// send to empty filesystem
func SendSnapshotFull(receiver string, end string) error {
	cmd := exec.Command("bash", "-c", "zfs send"+POOL_NAME+"@"+end+"| ssh"+receiver+"zfs recv "+POOL_NAME)
	log.Print(cmd.String())
	stderr, _ := cmd.StderrPipe()
	err := cmd.Start()
	if err != nil {
		log.Print(err)
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			fmt.Println(scanner.Text())
			return err
		}
	}
	return nil
}
