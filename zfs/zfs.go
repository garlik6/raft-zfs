package zfs

import (
	"fmt"
	"log"
	"os/exec"
	"strings"
	"time"
)

var POOL_NAME = "storage"

func CheckMountStatus() (string, error) {
	cmd := exec.Command("grep", POOL_NAME, "/proc/mounts")
	log.Print(cmd.String())
	res, err := cmd.CombinedOutput()
	if err != nil {
		return "", err
	}
	strres := string(res[:])
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

const lockFileName = "pool.lock"

func LockPool(ip string) error {
	sshCmd := fmt.Sprintf("ssh %s 'touch %s'", ip, lockFileName)
	cmd := exec.Command("bash", "-c", sshCmd)
	err := cmd.Run()
	log.Print(cmd.String())
	if err != nil {
		return err
	}
	return nil
}

func UnlockPool(ip string) error {
	sshCmd := fmt.Sprintf("ssh %s 'rm %s'", ip, lockFileName)
	cmd := exec.Command("bash", "-c", sshCmd)
	log.Print(cmd.String())
	err := cmd.Run()
	if err != nil {
		return err
	}
	return nil
}

func CheckLock(ip string) (bool, error) {
	sshCmd := fmt.Sprintf("ssh %s 'test -f %s && echo 1 || echo 0'", ip, lockFileName)
	cmd := exec.Command("bash", "-c", sshCmd)
	output, err := cmd.Output()
	log.Print(cmd.String())
	if err != nil {
		log.Print("pool is locked")
		return true, err
	}
	if string(output) == "0\n" {
		log.Print("pool is not locked")
		return false, nil
	} else {
		log.Print("pool is locked")
		return true, err
	}
}

// LockPool creates a lock file to indicate that the pool is locked on the target node.
func CheckPoolExistance() bool {
	cmd := exec.Command("zpool", "list", POOL_NAME)
	output, err := cmd.CombinedOutput()

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
		return "0", nil
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

func MountPool() error {
	if CheckPoolExistance() {
		cmd := exec.Command("sudo", "zfs", "mount", POOL_NAME)
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

func CreateSnapshot(number string) (error, bool) {
	cmd := exec.Command("zfs", "snapshot", POOL_NAME+"@"+number)
	log.Print(cmd.String())
	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Print(err)
		log.Print(string(output[:]))
		if strings.Contains(string(output[:]), "dataset already exists") {
			return nil, true
		} else {
			return err, false
		}
	}
	return nil, false
}

func DestroySnapshot(number string) error {
	cmd := exec.Command("zfs", "destroy", POOL_NAME+"@"+number)
	log.Print(cmd.String())
	out, err := cmd.CombinedOutput()
	if err != nil {
		log.Print(string(out[:]))
		return err
	}
	return nil
}

func SendSnapshotIncremental(receiver string, beg string, end string) (error, bool) {

	locked, err := CheckLock(receiver)
	if locked {
		log.Print("pool is locked continue...")
		return fmt.Errorf("pool is locked"), true
	}
	cmd := exec.Command("bash", "-c", "zfs send -i "+POOL_NAME+"@"+beg+" "+POOL_NAME+"@"+end+" | ssh "+receiver+" zfs recv "+POOL_NAME)
	log.Print("send from " + beg + " to " + end + " (receiver" + receiver + ")")
	log.Print(cmd.String())
	LockPool(receiver)
	start := time.Now()
	output, err := cmd.CombinedOutput()
	elapsed := time.Since(start)
	UnlockPool(receiver)
	log.Print("finished sending from " + beg + " to " + end + " (receiver" + receiver + ")" + "time:" + elapsed.String())
	if err != nil {
		log.Print(string(output[:]))
		return err, false
	}
	return nil, false
}

// send to empty filesystem
func SendSnapshotFull(receiver string, end string) error {
	cmd1 := exec.Command("bash", "-c", "ssh "+receiver+" sudo umount -l /"+POOL_NAME)
	cmd2 := exec.Command("bash", "-c", "ssh "+receiver+" sudo zfs destroy -r "+POOL_NAME)
	cmd3 := exec.Command("bash", "-c", "sudo zfs send "+POOL_NAME+"@"+end+" | ssh "+receiver+" zfs recv -F "+POOL_NAME)
	cmd4 := exec.Command("bash", "-c", "ssh "+receiver+" sudo zfs mount "+POOL_NAME)
	out1, err1 := cmd1.CombinedOutput()
	log.Print(cmd1.String())
	out2, err2 := cmd2.CombinedOutput()
	log.Print(cmd2.String())
	out3, err3 := cmd3.CombinedOutput()
	log.Print(cmd3.String())
	out4, err4 := cmd4.CombinedOutput()
	log.Print(cmd4.String())
	log.Print(string(out1[:]))
	log.Print(string(out2[:]))
	log.Print(string(out3[:]))
	log.Print(string(out4[:]))
	if err1 != nil {
		return err1
	}
	if err2 != nil {
		log.Print(string(out2[:]))
		return err1
	}
	if err3 != nil {
		log.Print(string(out3[:]))
		return err1
	}
	if err4 != nil {
		log.Print(string(out4[:]))
		return err1
	}
	return nil
}
