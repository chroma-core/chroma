// IGNORE THIS - go:generate protoc -I ../idl/chromadb/proto --go_out=./pkg/proto --go-grpc_out=./pkg/proto ../idl/chromadb/proto/*.proto
package main

//go:generate sh -c "ls ."
//go:generate sh -c "protoc -I ../idl/ --go_out=../go/pkg/proto/coordinatorpb --go_opt paths=source_relative --go-grpc_out=../go/pkg/proto/coordinatorpb --go-grpc_opt paths=source_relative ../idl/chromadb/proto/chroma.proto ../idl/chromadb/proto/coordinator.proto ../idl/chromadb/proto/logservice.proto"

//go:generate sh -c "mv ../go/pkg/proto/coordinatorpb/chromadb/proto/logservice*.go ../go/pkg/proto/logservicepb/"

//go:generate sh -c "mv ../go/pkg/proto/coordinatorpb/chromadb/proto/*.go ../go/pkg/proto/coordinatorpb/"

//go:generate sh -c "rm -rf ../go/pkg/proto/coordinatorpb/chromadb"

func main() {}
