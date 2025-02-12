package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"path"
	"runtime/debug"
	"time"

	"github.com/jacobsa/fuse"
	"github.com/radek-ryckowski/monofs/fs/config"
	monostat "github.com/radek-ryckowski/monofs/monoclient/stat"
	monostatserver "github.com/radek-ryckowski/monofs/monoserver/stat"
	"github.com/radek-ryckowski/monofs/worker"
	"go.uber.org/zap"
)

var fMountPoint = flag.String("mount_point", "", "Path to mount point.")
var fInodePath = flag.String("inode_path", "/tmp/monofs", "Path to metadata store.")
var fReadOnly = flag.Bool("read_only", false, "Mount in read-only mode.")
var fStatServerAddress = flag.String("statAddress", "", "Address of stat backend server.")
var fCertDir = flag.String("cert_dir", "", "Certificate directory")
var fDev = flag.Bool("dev", false, "Run in development mode")
var fFuseDebug = flag.Bool("fuse_debug", false, "Run in fuse debug mode")
var fManagerPort = flag.String("manager_port", ":50052", "Manager port")
var fCacheSize = flag.Int("cache_size", 100, "Cache size") //was 10000
var fShutdownTimeout = flag.Duration("shutdown_timeout", 60*time.Second, "Shutdown timeout")
var fFilesystemName = flag.String("filesystem_name", "monofs#head", "Filesystem name")
var fBloomFilterSize = flag.Int("bloom_filter_size", 10000, "Bloom filter size")
var fLocalDataPath = flag.String("local_data_path", "", "Local data path")

func version() string {
	var (
		rev = "unknown"
	)
	buildInfo, ok := debug.ReadBuildInfo()
	if !ok {
		return rev
	}
	for _, v := range buildInfo.Settings {
		if v.Key == "vcs.revision" {
			rev = v.Value
			break
		}
	}
	return rev
}

func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close()

	return l.Addr().(*net.TCPAddr).Port, nil
}

func main() {
	flag.Parse()
	logger, err := zap.NewProduction()
	if *fDev {
		logger, err = zap.NewDevelopment()
	}
	sugarlog := logger.Sugar()

	if err != nil {
		log.Fatalf("Failed to initialize zap logger: %v", err)
	}

	if *fMountPoint == "" {
		log.Fatalf("You must set --mount_point.")
	}

	if *fStatServerAddress == "" {
		if *fDev {
			port, err := getFreePort()
			if err != nil {
				log.Fatalf("Failed to get free port: %v", err)
			}
			*fStatServerAddress = fmt.Sprintf("localhost:%d", port)
			// start stat server in separate goroutine
			go func() {
				statSrv := monostatserver.New()
				if err := statSrv.Start(fmt.Sprintf(":%d", port), *fCertDir, sugarlog); err != nil {
					log.Fatalf("Failed to start stat server: %v", err)
				}
			}()
		} else {
			log.Fatalf("You must set --address.")
		}
	}
	fuseCfg := &fuse.MountConfig{
		ReadOnly:    *fReadOnly,
		ErrorLogger: zap.NewStdLog(sugarlog.Desugar()),
		FSName:      *fFilesystemName,
	}

	if *fFuseDebug {
		fuseCfg.DebugLogger = zap.NewStdLog(sugarlog.Desugar())
	}
	// Create a connection to the stat server.
	conn, err := monostat.NewConnection(*fStatServerAddress, *fCertDir, sugarlog)
	if err != nil {
		log.Fatalf("Stat connection : %v", err)
	}

	localDataPath := *fLocalDataPath
	if localDataPath == "" {
		localDataPath = path.Join(*fInodePath, "localDataPath")
	}
	// TODO  add possibility to read config from file instead from flags
	worker, err := worker.New(&config.Config{
		Path:            *fInodePath,
		FilesystemName:  *fFilesystemName,
		StatClient:      monostat.New(conn),
		FuseCfg:         fuseCfg,
		Mountpoint:      *fMountPoint,
		DebugMode:       *fDev,
		ReadOnly:        *fReadOnly,
		ShutdownTimeout: *fShutdownTimeout,
		CacheSize:       *fCacheSize,
		ManagerPort:     *fManagerPort,
		BloomFilterSize: *fBloomFilterSize,
		LocalDataPath:   localDataPath,
	}, sugarlog)
	if err != nil {
		log.Fatalf("makeFS: %v", err)
	}
	if err := worker.Start(); err != nil {
		log.Fatalf("Start: %v", err)
	}
	sugarlog.Infof("Started monofs version %s", version())
	worker.Wait()
}
