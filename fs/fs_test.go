package monofs

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"os"
	"path"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/jacobsa/fuse/fusetesting"
	"github.com/jacobsa/fuse/samples"
	"go.uber.org/zap"

	//	. "github.com/jacobsa/oglematchers"
	. "github.com/jacobsa/ogletest"
	"github.com/radek-ryckowski/monofs/fs/config"
	monostat "github.com/radek-ryckowski/monofs/monoclient/stat"
	pb "github.com/radek-ryckowski/monofs/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

var (
	lis             *bufconn.Listener
	blocksAvailable uint64
)

type FakeStatServer struct {
	BlocksAvailable uint64
	pb.UnimplementedMonofsStatServer
}

func (f *FakeStatServer) Stat(ctx context.Context, in *pb.StatRequest) (*pb.StatResponse, error) {
	return &pb.StatResponse{
		Id:              "",
		BlockSize:       4096,
		Blocks:          f.BlocksAvailable,
		BlocksFree:      uint64(float64(f.BlocksAvailable) * 0.9),
		BlocksAvailable: f.BlocksAvailable,
	}, nil
}

func NewFakeStatServer(blocksAvailable uint64) *FakeStatServer {
	return &FakeStatServer{
		BlocksAvailable: blocksAvailable,
	}
}

func randUint64() uint64 {
	rand.Seed(time.Now().UnixNano())
	n := rand.Uint64()
	return n + 1024*1024
}

func TestMonoFS(t *testing.T) { RunTests(t) }

type MonoFSTest struct {
	samples.SampleTest
	inodePath string
}

func init() { RegisterTestSuite(&MonoFSTest{}) }

func bufDialer(context.Context, string) (net.Conn, error) {
	return lis.Dial()
}

func (t *MonoFSTest) SetUp(ti *TestInfo) {
	var err error
	os.Setenv("MONOFS_DEV_RUN", "MonoFSTest")
	t.inodePath, err = os.MkdirTemp("", "monofs_inodepath")
	AssertEq(nil, err)
	lis = bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	blocksAvailable = randUint64()
	pb.RegisterMonofsStatServer(grpcServer, NewFakeStatServer(blocksAvailable))
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			panic(err)
		}
	}()
	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(bufDialer), grpc.WithTransportCredentials(insecure.NewCredentials()))
	AssertEq(nil, err)
	logger, err := zap.NewProduction()
	AssertEq(nil, err)
	sugarlog := logger.Sugar()

	fs, err := NewMonoFS(&config.Config{
		Path:           t.inodePath,
		FilesystemName: "test",
		StatClient:     monostat.New(conn),
	},
		sugarlog,
	)
	AssertEq(nil, err)
	t.Server, err = NewMonoFuseFS(fs)
	AssertEq(nil, err)
	t.SampleTest.SetUp(ti)
}

func (t *MonoFSTest) TearDown() {
	os.RemoveAll(t.Dir)
}

func (t *MonoFSTest) ReadDir_Root() {
	err := os.Mkdir(t.Dir+"/foo_root", 0755)
	AssertEq(nil, err)
	err = os.Mkdir(t.Dir+"/bar_root", 0755)
	AssertEq(nil, err)
	entries, err := fusetesting.ReadDirPicky(t.Dir)
	AssertEq(nil, err)
	AssertEq(2, len(entries))
}

func (t *MonoFSTest) StatFs() {
	stat := syscall.Statfs_t{}
	err := syscall.Statfs(t.Dir, &stat)
	AssertEq(nil, err)
	AssertEq(stat.Blocks, blocksAvailable)
	AssertEq(stat.Bfree, uint64(float64(blocksAvailable)*0.9))
}

// CreateRandomString creates random string with given length
func CreateRandomString(length int) string {
	rand.Seed(time.Now().UnixNano())
	letterRunes := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	b := make([]rune, length)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func CreateAndCheckFileTree(rootPath, dir string, round int) error {
	if err := os.Mkdir(rootPath+dir, 0755); err != nil {
		return fmt.Errorf("round %d failed to create dir %s: %w", round, dir, err)
	}
	if err := os.Mkdir(rootPath+dir+"/baz", 0755); err != nil {
		return fmt.Errorf("round %d failed to create dir %s: %w", round, dir+"/baz", err)
	}
	if err := os.Mkdir(rootPath+dir+"/baz/qux", 0755); err != nil {
		return fmt.Errorf("round %d failed to create dir %s: %w", round, dir+"/baz/qux", err)
	}
	file, err := os.Create(rootPath + dir + "/baz/qux/file.txt")
	if err != nil {
		return fmt.Errorf("round %d failed to create file %s: %w", round, dir+"/baz/qux/file.txt", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("round %d failed to close file %s: %w", round, dir+"/baz/qux/file.txt", err)
	}
	file, err = os.Create(rootPath + dir + "/baz/qux/file2.txt")
	if err != nil {
		return fmt.Errorf("round %d failed to create file %s: %w", round, dir+"/baz/qux/file2.txt", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("round %d failed to close file %s: %w", round, dir+"/baz/qux/file2.txt", err)
	}
	filesCnt := 0
	dirCnt := 0
	err = filepath.Walk(rootPath+dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			dirCnt++
		} else {
			filesCnt++
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("round %d failed to walk %s: %w", round, dir, err)
	}
	if filesCnt != 2 {
		return fmt.Errorf("round %d filesCnt(%d) != 2", round, filesCnt)
	}
	if dirCnt != 3 {
		return fmt.Errorf("round %d dirCnt(%d) != 3", round, dirCnt)
	}
	return os.RemoveAll(rootPath + dir)
}

func (t *MonoFSTest) RewriteFiles() {
	testDir := fmt.Sprintf("/%s", CreateRandomString(64))
	for i := 0; i < 20; i++ {
		err := CreateAndCheckFileTree(t.Dir, testDir, i)
		AssertEq(nil, err)
		entries, err := fusetesting.ReadDirPicky(t.Dir)
		AssertEq(nil, err)
		AssertEq(0, len(entries))
		time.Sleep(100 * time.Millisecond)
	}
}

func (t *MonoFSTest) CreateRemoveLinks() {
	err := os.Mkdir(t.Dir+"/foo", 0755)
	AssertEq(nil, err)
	err = os.Mkdir(t.Dir+"/bar", 0755)
	AssertEq(nil, err)
	err = os.Mkdir(t.Dir+"/baz", 0755)
	AssertEq(nil, err)
	err = os.Symlink(t.Dir+"/foo", t.Dir+"/bar/foo")
	AssertEq(nil, err)
	err = os.Symlink(t.Dir+"/foo", t.Dir+"/baz/foo")
	AssertEq(nil, err)
	err = os.Remove(t.Dir + "/bar/foo")
	AssertEq(nil, err)
	err = os.Remove(t.Dir + "/baz/foo")
	AssertEq(nil, err)
	entries, err := fusetesting.ReadDirPicky(t.Dir)
	AssertEq(nil, err)
	AssertEq(3, len(entries))
}

func (t *MonoFSTest) Create2Kfiles1Dir() {
	dirPath := path.Join(t.Dir, "1Dir")
	err := os.Mkdir(dirPath, 0755)
	AssertEq(nil, err)
	for i := 0; i < 2000; i++ {
		fPath := path.Join(dirPath, fmt.Sprintf("file%d.txt", i))
		myfile, err := os.Create(fPath)
		AssertEq(nil, err)
		myfile.Close()
	}

	for i := 0; i < 2000; i++ {
		fPath := path.Join(dirPath, fmt.Sprintf("file%d.txt", i))
		err := os.Remove(fPath)
		AssertEq(nil, err)
	}
}
