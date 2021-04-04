package main

import (
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"

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

func main() {
	go func() {
		http.ListenAndServe("0.0.0.0:8082", nil)

	}()
	addr := "0.0.0.0:9001"
	if len(os.Args) == 2 {
		addr = os.Args[1]
	}
	listen, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}
	server := md5simd.NewServer()
	for {
		conn, err := listen.Accept()
		if err != nil {
			panic(err)
		}
		go handle(conn, server.NewHash())

	}
}
func handle(conn net.Conn, md5 md5simd.Hasher) {
	l := make([]byte, 4)
	var r Result
	for {
		_, err := io.ReadFull(conn, l)
		if err != nil {
			panic(err)
		}
		msglen := int(l[0]) | int(l[1])<<8 | int(l[2])<<16 | int(l[3])<<24
		b := make([]byte, msglen)
		_, err = io.ReadFull(conn, b)
		if err != nil {
			panic(err)
		}
		var f *FileInfo
		err = json.Unmarshal(b, &f)
		if err != nil {
			panic(err)
		}
		if f == nil {
			panic("读取错误")
		}
		if f.Os == "windows" {
			if runtime.GOOS == "linux" {
				f.Path = strings.ReplaceAll(f.Path, `\`, "/")
			}
		} else if f.Os == "linux" {
			if runtime.GOOS == "windows" {
				f.Path = strings.ReplaceAll(f.Path, `/`, `\`)
			}
		}
		r.Code = 0
		r.Path = f.Path

		s, err := os.Stat(f.Path)
		if err != nil {
			if f.IsDir {
				os.Mkdir(f.Path, 0777)
			} else {
				r.Code = 1
			}
		} else {
			if f.IsDir {
				if !s.IsDir() {
					os.RemoveAll(f.Path)
				}
			} else { //文件
				if s.IsDir() {
					dir, err := ioutil.ReadDir(f.Path)
					if err != nil {
						panic(err)
					}
					for _, d := range dir {
						os.RemoveAll(path.Join([]string{"tmp", d.Name()}...))
					}
					r.Code = 1
				} else {
					if f.Size != s.Size() {
						r.Code = 1
						fmt.Println(f.Path, "文件不一致")
					} else if f.ChecksumType != "" {
						if f.Checksum == "" {
							panic(f.Path + "client Checksum为空")
						}
						checksum := getChecksum(f.Path, f.ChecksumType, md5)
						if checksum == "" {
							panic(f.Path + "server Checksum为空")
						}
						if checksum != f.Checksum {
							r.Code = 1
							fmt.Println(f.Path, "文件Checksum不一致", checksum, f.Checksum)
						}
					}
				}
			}

		}

		//远程专用
		var dest *os.File
		if r.Code == 1 {
			dest, err = os.Create(f.Path)
			if err != nil {
				r.Code = 0
				panic(err)
			}
		}
		b, _ = json.Marshal(r)
		msglen = len(b)
		msg := make([]byte, 4+msglen)
		msg[0] = byte(msglen)
		msg[1] = byte(msglen >> 8)
		msg[2] = byte(msglen >> 16)
		msg[3] = byte(msglen >> 24)
		copy(msg[4:], b)
		conn.Write(msg)
		if r.Code == 1 {
			for {
				_, err := io.ReadFull(conn, l)
				if err != nil {
					panic(err)
				}
				msglen := int(l[0]) | int(l[1])<<8 | int(l[2])<<16 | int(l[3])<<24
				if msglen == 0 {
					break
				}
				b := make([]byte, msglen)
				_, err = io.ReadFull(conn, b)
				if err != nil {
					panic(err)
				}
				dest.Write(b)
				dest.Sync()
			}
		}
		if dest != nil {
			dest.Close()
		}
		if r.Code == 1 {
			if f.ChecksumType != "" && f.Checksum != getChecksum(f.Path, f.ChecksumType, md5) {
				fmt.Println(f.Path, "传输后校验和不正确")
			}
		}
	}
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
		}
		res = hex.EncodeToString(md5.Sum(nil))
	}
	return res
}
