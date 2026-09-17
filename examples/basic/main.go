// Command basic writes, reads, and removes one object using explicit settings.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	rados "github.com/otuschhoff/rados-go"
)

func main() {
	if len(os.Args) != 3 {
		log.Fatalf("usage: %s POOL OBJECT", os.Args[0])
	}
	monitors := strings.Fields(os.Getenv("RADOS_MONITORS"))
	entity := os.Getenv("RADOS_ENTITY")
	fsid := os.Getenv("RADOS_FSID")
	key := os.Getenv("RADOS_KEY")
	if len(monitors) == 0 || entity == "" || fsid == "" || key == "" {
		log.Fatal("RADOS_MONITORS, RADOS_ENTITY, RADOS_FSID, and RADOS_KEY are required")
	}
	client, err := rados.New(rados.Config{
		Monitors:    monitors,
		Entity:      entity,
		ClusterFSID: fsid,
		Key:         []byte(key),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		log.Fatal(err)
	}
	pool, err := client.OpenPool(ctx, os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	object := pool.Object(os.Args[2])
	if _, err := object.Create(ctx, true); err != nil && !errors.Is(err, rados.ErrExists) {
		log.Fatal(err)
	}
	if _, err := object.WriteFull(ctx, []byte("hello from pure Go\n")); err != nil {
		log.Fatal(err)
	}
	data, info, err := object.Read(ctx, 0, 1<<20)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("version=%d size=%d data=%q\n", info.Version, info.Size, data)
	if _, err := object.Remove(ctx); err != nil {
		log.Fatal(err)
	}
}