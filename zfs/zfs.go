package zfs

import (
	"bufio"
	"fmt"
	"log"
	"os/exec"
	"strconv"
	"strings"
)

var POOL_NAME = "storage"

func CheckMountStatus() (string, error) {
	cmd := exec.Command("grep", POOL_NAME, "/proc/mounts")
	log.Print(cmd.String())
	res, err := cmd.Output()
	if err != nil {
		return "", err
	}
	strres := string(res)
	log.Print(strres)
	if len(strres) == 0 {
		return "UM", err
	}
	if strings.Contains(strres, "ro") {
		return "RO", err
	}
	if strings.Contains(strres, "rw") {
		return "RW", err
	}
	return "", fmt.Errorf("wrong format")
}

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

func GetLastSnapNumber(target string) (string, error) {
	cmd := exec.Command("ssh", "admin@"+target, "zfs", "list", "-t", "snap", "-o", "name")
	log.Print(cmd.String())
	result, err := cmd.Output()
	log.Print(string(result))
	if err != nil {
		return "", err
	}
	if string(result) == "" {
		return "-1", nil
	}
	strArray := strings.Split(string(result), "\n")
	log.Print(len(strArray))
	lastLine := strArray[len(strArray)-2]
	log.Print(lastLine)
	indexOf := strings.LastIndex(lastLine, "@")
	return lastLine[indexOf+1:], nil
}

func UmountPool() error {
	if CheckPoolExistance() {
		cmd := exec.Command("sudo", "zfs", "umount", POOL_NAME)
		log.Print(cmd.String())
		err := cmd.Run()
		if err != nil {
			log.Print(err)
			return err
		}
	}
	return nil
}

func SetRW() error {
	if CheckPoolExistance() {
		cmd := exec.Command("sudo", "zfs", "set", "readonly=off", POOL_NAME)
		log.Print(cmd.String())
		err := cmd.Run()
		if err != nil {
			log.Print(err)
			return err
		}
	}
	return nil
}

func SetRO() error {
	if CheckPoolExistance() {
		cmd := exec.Command("sudo", "zfs", "set", "readonly=on", POOL_NAME)
		log.Print(cmd.String())
		err := cmd.Run()
		if err != nil {
			log.Print(err)
			return err
		}
	}
	return nil
}

func CreatePool() error {
	if !CheckPoolExistance() {
		cmd := exec.Command("zpool", "create", POOL_NAME, "/dev/vdb")
		err := cmd.Run()
		if err != nil {
			log.Print(err)
			return err
		}
	} else {
		fmt.Errorf("pool exists")
	}
	return nil
}

func CreateSnapshot(number string, logs chan string) error {
	lastSnapNum, _ := strconv.Atoi(number)
	newSnap := strconv.Itoa(lastSnapNum)
	cmd := exec.Command("zfs", "snapshot", POOL_NAME+"@"+newSnap)
	log.Print(cmd.String())
	stderr, _ := cmd.StderrPipe()
	err := cmd.Run()
	if err != nil {
		log.Print(err)
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			fmt.Println(scanner.Text())
		}
		return err
	}
	return nil
}

func DestroySnapshot(number string) error {
	lastSnapNum, _ := strconv.Atoi(number)
	newSnap := strconv.Itoa(lastSnapNum)
	cmd := exec.Command("zfs", "destroy", POOL_NAME+"@"+newSnap)
	log.Print(cmd.String())
	stderr, _ := cmd.StderrPipe()
	err := cmd.Run()
	if err != nil {
		log.Print(err)
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			fmt.Println(scanner.Text())
		}
		return err
	}
	return nil
}

func SendSnapshotIncremental(receiver string, beg string, end string) error {
	cmd := exec.Command("bash", "-c", "zfs send -i "+POOL_NAME+"@"+beg+" "+POOL_NAME+"@"+end+" | ssh "+receiver+" zfs recv "+POOL_NAME)
	log.Print("send from " + beg + " to " + end + " (receiver" + receiver + ")")
	log.Print(cmd.String())
	stderr, _ := cmd.StderrPipe()
	stdout, _ := cmd.StdoutPipe()
	err := cmd.Start()
	scanner := bufio.NewScanner(stdout)
	i := 0
	for scanner.Scan() {
		i++
		fmt.Println(scanner.Text())
	}
	if i != 0 {
		return fmt.Errorf("unable to send snapshot")
	}
	if err != nil {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			fmt.Println(scanner.Text())
		}
		return err
	}
	return nil
}

// send to empty filesystem
func SendSnapshotFull(receiver string, end string) error {
	cmd1 := exec.Command("bash", "-c", "ssh "+receiver+" sudo zfs umount "+POOL_NAME)
	cmd2 := exec.Command("bash", "-c", "zfs send "+POOL_NAME+"@"+end+" | ssh "+receiver+" zfs recv -F "+POOL_NAME)
	cmd3 := exec.Command("bash", "-c", "ssh "+receiver+" sudo zfs mount "+POOL_NAME)
	log.Print(cmd1.String())
	log.Print(cmd2.String())
	log.Print(cmd3.String())
	err1 := cmd1.Start()
	err2 := cmd2.Start()
	err3 := cmd3.Start()
	if err1 != nil {
		log.Print(err1)
		return err1
	}
	if err2 != nil {
		log.Print(err1)
		return err1
	}
	if err3 != nil {
		log.Print(err1)
		return err1
	}
	return nil
}
