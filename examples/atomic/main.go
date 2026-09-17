// Command atomic performs one version-guarded compound object update.
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
	info, err := object.Stat(ctx)
	if errors.Is(err, rados.ErrNotFound) {
		log.Fatal("object must already exist")
	}
	if err != nil {
		log.Fatal(err)
	}

	operation := rados.NewWriteOp()
	operation.AssertVersion(info.Version)
	operation.SetXAttr("example.state", []byte("updated"))
	operation.Append([]byte("updated\n"))
	result, err := object.ExecuteWrite(ctx, operation)
	if errors.Is(err, rados.ErrConflict) {
		log.Fatal("object changed before the update was committed")
	}
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("updated object version %d -> %d\n", info.Version, result.Version)
}
