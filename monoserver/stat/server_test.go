package stat

import (
	pb "github.com/radek-ryckowski/monofs/proto"
)

type FakeStatServer struct {
	pb.UnimplementedMonofsStatServer
}

func NewFakeStatServer() *FakeStatServer {
	return &FakeStatServer{}
}
