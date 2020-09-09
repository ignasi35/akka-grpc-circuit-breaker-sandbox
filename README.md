# Sandbox to test CircuitBreakers in Akka-gRPC 

See the code at `GreeterClient`. Look for the `Interceptor`. Refer to https://github.com/akka/akka-grpc/pull/1118.

Run a client and then a server.

Kill the server and see the traces on the client terminal.