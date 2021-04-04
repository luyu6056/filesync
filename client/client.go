package main

import (
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"io/ioutil"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	json "github.com/json-iterator/go"
	md5simd "github.com/minio/md5-simd"
)

type FileInfo struct {
	Path         string
	IsDir        bool
	ModTime      int64
	Size         int64
	Body         []byte
	Os           string
	ChecksumType string
	Checksum     string
}
type Result struct {
	Code int
	Path string
}
type work struct {
	s    fs.FileInfo
	path string
}

var workDir = make(chan work, 1000000)
var workFile = make(chan work, 100)
var send = make(chan []byte, 8*8)
var checksumType string

func main() {
	if len(os.Args) == 1 {
		panic("没有ip")
		return
	}
	go func() {
		http.ListenAndServe("0.0.0.0:8081", nil)

	}()
	s, _ := os.Stat(".")
	workDir <- work{
		s:    s,
		path: ".",
	}
	ip := os.Args[1]
	if len(os.Args) == 3 {
		checksumType = os.Args[2]
		if checksumType != "-crc32" && checksumType != "-md5" {
			panic("只支持-crc32与-md5")
		}
	}
	md5server := md5simd.NewServer()
	for i := 0; i < 8; i++ {
		go checkDir()
		go checkFile(md5server.NewHash())
		conn, err := net.Dial("tcp", ip)
		if err != nil {
			panic(err)
		}
		go sendMsg(conn)

	}
	fmt.Println("连接", ip)
	print()
}

func sendMsg(conn net.Conn) {
	lb := make([]byte, 4)
	for b := range send {
		connSend(conn, b)
		_, err := io.ReadFull(conn, lb)
		if err != nil {
			panic(err)
		}
		msglen := int64(lb[0]) | int64(lb[1])<<8 | int64(lb[2])<<16 | int64(lb[3])<<24
		b = make([]byte, msglen)
		_, err = io.ReadFull(conn, b)
		if err != nil {
			panic(err)
		}
		var r *Result
		err = json.Unmarshal(b, &r)
		if err != nil {
			panic(err)
		}
		if r == nil {
			panic("读取错误")
		}
		if r.Code == 1 {
			//filecopy(r.Path, `/mnt/sdc1/data`+r.Path[1:])
			sendfile(conn, r.Path)
		}
	}
}

var n, oldn, checkn, oldcheckn uint64
var oldsize, size uint64
var begin = time.Now()
var oldtime = time.Now().Unix()

func print() {
	tick := time.NewTicker(time.Second * 10)
	for now := range tick.C {
		if checksumType == "" {
			fmt.Printf("花费%v,已完成%v,io:%0.2f/s,传输速度:%0.2fm/s\r\n", time.Since(begin), n, float64(n-oldn)/float64(now.Unix()-oldtime), float64(size-oldsize)/1024/1024/float64(now.Unix()-oldtime))
		} else {
			fmt.Printf("花费%v,已完成%v,io:%0.2f/s,传输速度:%0.2fm/s,checksum速度:%0.2fm/s\r\n", time.Since(begin), n, float64(n-oldn)/float64(now.Unix()-oldtime), float64(size-oldsize)/1024/1024/float64(now.Unix()-oldtime), float64(checkn-oldcheckn)/1024/1024/float64(now.Unix()-oldtime))
		}

		oldtime = now.Unix()
		oldsize = size
		oldn = n
		oldcheckn = checkn
	}
}
func checkDir() {
	for w := range workDir {
		if w.path == `.\$RECYCLE.BIN` {
			continue
		}
		atomic.AddUint64(&n, 1)
		b, _ := json.Marshal(FileInfo{
			IsDir: true,
			Path:  w.path,
			Os:    runtime.GOOS,
		})
		send <- b
		files, _ := ListDir(w.path, "")
		for _, f := range files {
			s, err := os.Stat(f)
			if err != nil {
				continue
			}
			w := work{
				s:    s,
				path: f,
			}
			if s.IsDir() {
				workDir <- w
			} else {
				workFile <- w
			}

		}
	}
}
func checkFile(md5 md5simd.Hasher) {
	for w := range workFile {
		if w.path == `.\client.go` || w.path == `.\client.exe` || w.path == `./client` {
			continue
		}

		atomic.AddUint64(&n, 1)
		b, _ := json.Marshal(FileInfo{
			Path:         w.path,
			ModTime:      w.s.ModTime().Unix(),
			Size:         w.s.Size(),
			Os:           runtime.GOOS,
			Checksum:     getChecksum(w.path, checksumType, md5),
			ChecksumType: checksumType,
		})
		send <- b
	}
}

//获取指定目录下的所有文件，不进入下一级目录搜索，可以匹配后缀过滤。
func ListDir(dirPth string, suffix string) (files []string, err error) {
	files = make([]string, 0, 10)
	dir, err := ioutil.ReadDir(dirPth)
	if err != nil {
		return nil, err
	}
	PthSep := "/"
	if os.IsPathSeparator('\\') { //前边的判断是否是系统的分隔符
		PthSep = "\\"
	}
	suffix = strings.ToUpper(suffix) //忽略后缀匹配的大小写
	for _, fi := range dir {

		if strings.HasSuffix(strings.ToUpper(fi.Name()), suffix) { //匹配文件
			files = append(files, dirPth+PthSep+fi.Name())
		}
	}
	return files, nil
}

func filecopy(src, dst string) error {
	sourceFileStat, err := os.Stat(src)
	if err != nil {
		return err
	}

	if !sourceFileStat.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", src)
	}

	source, err := os.Open(src)
	if err != nil {
		return err
	}
	defer source.Close()

	destination, err := os.Create(dst)
	if err != nil {
		return err
	}

	defer destination.Close()
	buf := make([]byte, 10*1024*1014)
	for {
		n, err := source.Read(buf)
		if err != nil && err != io.EOF {
			return err
		}
		if n == 0 {
			break
		}

		if _, err := destination.Write(buf[:n]); err != nil {
			return err
		}
		atomic.AddUint64(&size, uint64(checkn))
	}
	return nil
}
func getChecksum(src string, typ string, md5 md5simd.Hasher) (res string) {

	switch typ {
	case "-crc32":
		check := crc32.NewIEEE()
		source, err := os.Open(src)
		if err != nil {
			return ""
		}
		defer source.Close()
		buf := make([]byte, 8<<20)
		for {
			n, err := source.Read(buf)
			if err != nil && err != io.EOF {
				return ""
			}
			if n == 0 {
				break
			}
			check.Write(buf[:n])
			atomic.AddUint64(&checkn, uint64(n))
		}
		res = strconv.Itoa(int(check.Sum32()))
	case "-md5":
		md5.Reset()
		source, err := os.Open(src)
		if err != nil {
			return ""
		}
		defer source.Close()
		buf := make([]byte, 8<<20)
		for {
			n, err := source.Read(buf)
			if err != nil && err != io.EOF {
				return ""
			}
			if n == 0 {
				break
			}
			md5.Write(buf[:n])
			atomic.AddUint64(&checkn, uint64(n))
		}
		res = hex.EncodeToString(md5.Sum(nil))
	}
	return res
}
func sendfile(conn net.Conn, src string) error {
	source, err := os.Open(src)
	if err != nil {
		return err
	}
	defer source.Close()

	buf := make([]byte, 10*1024*1014)
	for {
		n, err := source.Read(buf)
		if err != nil && err != io.EOF {
			return err
		}
		connSend(conn, buf[:n])
		if n == 0 {
			break
		}

		atomic.AddUint64(&size, uint64(n))
	}
	return nil
}
func connSend(conn net.Conn, b []byte) {
	l := len(b)
	msg := make([]byte, 4+l)
	msg[0] = byte(l)
	msg[1] = byte(l >> 8)
	msg[2] = byte(l >> 16)
	msg[3] = byte(l >> 24)
	copy(msg[4:], b)
	conn.Write(msg)
}
