package main

import (
	"errors"
	"fmt"
	bitcask "github.com/XiXi-2024/xixi-bitcask-kv"
	"github.com/gin-gonic/gin"
	"golang.org/x/exp/slog"
	"net/http"
	"os"
	"strings"
)

var db *bitcask.DB

func init() {
	var err error
	options := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-http")
	options.DirPath = dir
	db, err = bitcask.Open(options)
	if err != nil {
		panic(fmt.Sprintf("failed to open db:%v", err))
	}
}

func handlePut(c *gin.Context) {
	var data map[string]string
	err := c.ShouldBind(&data)
	if err != nil {
		c.String(http.StatusBadRequest, fmt.Sprintf("parse error: %v", err))
	}

	for key, value := range data {
		err := db.Put([]byte(key), []byte(value))
		if err != nil {
			c.String(http.StatusInternalServerError, err.Error())
			slog.Error("failed to put:%v", key)
			return
		}
	}
}
func handleGet(c *gin.Context) {
	key := c.Query("key")
	value, err := db.Get([]byte(key))
	if err != nil && !errors.Is(err, bitcask.ErrKeyNotFound) {
		c.String(http.StatusInternalServerError, err.Error())
		slog.Error("failed to get value:%v", key)
		return
	}
	c.Header("Content-Type", "application/json")
	c.String(http.StatusOK, string(value))
}

func handleDelete(c *gin.Context) {
	key := c.Query("key")
	err := db.Delete([]byte(key))
	if err != nil && !errors.Is(err, bitcask.ErrKeyNotFound) {
		c.String(http.StatusInternalServerError, err.Error())
		slog.Error("failed to get value: %v", key)
		return
	}
	c.Header("Content-Type", "application/json")
	c.String(http.StatusOK, "OK")
}

func handleListKeys(c *gin.Context) {
	keys := db.ListKeys()
	c.Header("Content-Type", "application/json")
	var result []string
	for _, key := range keys {
		result = append(result, string(key))
	}
	c.String(http.StatusOK, strings.Join(result, "\n"))
}

func handleStat(c *gin.Context) {
	stat := db.Stat()
	c.Header("Content-Type", "application/json")
	c.JSON(http.StatusOK, stat)

}
func main() {
	engine := gin.Default()
	engine.NoMethod(func(context *gin.Context) {
		context.String(http.StatusMethodNotAllowed, "method not allowed")
	})
	engine.NoRoute(func(context *gin.Context) {
		context.String(http.StatusNotFound, "not found")
	})

	// 注册处理方法
	engine.POST("/bitcask/put", handlePut)
	engine.GET("/bitcask/get", handleGet)
	engine.DELETE("/bitcask/delete", handleDelete)
	engine.GET("/bitcask/list", handleListKeys)
	engine.GET("/bitcask/stat", handleStat)
	if err := engine.Run(":8080"); err != nil {
		panic(err)
	}
}
