module protobus-cpp/crosslang/gopeer

go 1.25

require (
	github.com/ArielLaub/protobus-go/v2 v2.0.0
	google.golang.org/protobuf v1.36.12
)

require github.com/rabbitmq/amqp091-go v1.15.0 // indirect

// The suite points this at the protobus-go checkout under test (PROTOBUS_GO).
replace github.com/ArielLaub/protobus-go/v2 => ../../../../protobus-go
