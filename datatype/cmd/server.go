package main

import (
	bitcask "github.com/XiXi-2024/xixi-kv"
	bitcask_datatype "github.com/XiXi-2024/xixi-kv/datatype"
	"github.com/tidwall/redcon"
	"log"
	"sync"
)

const addr = "127.0.0.1:6380"

// BitcaskServer Bitcask 服务端
type BitcaskServer struct {
	dbs    map[int]*bitcask_datatype.DataTypeService // 数据类型服务, 支持多个db实例
	server *redcon.Server                            // Redis 服务端实例
	mu     sync.RWMutex                              // 互斥锁
}

func main() {
	// 初始化数据类型服务实例
	dataTypeService, err := bitcask_datatype.NewDataTypeService(bitcask.DefaultOptions)
	if err != nil {
		panic(err)
	}

	// 初始化 Bitcask 服务端
	bitcaskServer := &BitcaskServer{
		dbs: make(map[int]*bitcask_datatype.DataTypeService),
	}
	bitcaskServer.dbs[0] = dataTypeService

	// 初始化 Redis 服务端
	bitcaskServer.server = redcon.NewServer(addr, execClientCommand, bitcaskServer.accept, bitcaskServer.close)
	// 执行监听
	bitcaskServer.listen()
}

// 启动 Redis 服务端
func (svr *BitcaskServer) listen() {
	log.Println("bitcask server running, ready to accept connections.")
	_ = svr.server.ListenAndServe()
}

// 接收到连接的处理
func (svr *BitcaskServer) accept(conn redcon.Conn) bool {
	// 创建对应的 Bitcask 客户端实例
	cli := new(BitcaskClient)
	svr.mu.Lock()
	defer svr.mu.Unlock()
	cli.server = svr
	// todo 仅选择首个 db 实例
	cli.db = svr.dbs[0]
	// 设置为上下文
	conn.SetContext(cli)
	return true
}

// 断开连接后的处理
func (svr *BitcaskServer) close(conn redcon.Conn, err error) {
	// 将所有打开的 db 实例关闭
	for _, db := range svr.dbs {
		_ = db.Close()
	}
}
