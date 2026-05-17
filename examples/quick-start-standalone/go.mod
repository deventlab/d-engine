module github.com/deventlab/d-engine/examples/quick-start-standalone

go 1.25.0

replace github.com/deventlab/d-engine/proto => ../../d-engine-proto/go

require (
	github.com/deventlab/d-engine/proto v0.0.0
	google.golang.org/grpc v1.80.0
)

require (
	golang.org/x/net v0.53.0 // indirect
	golang.org/x/sys v0.43.0 // indirect
	golang.org/x/text v0.36.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260406210006-6f92a3bedf2d // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)
