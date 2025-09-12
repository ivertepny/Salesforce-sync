Salesforce Pub/Sub API (gRPC) — Python stubs
--------------------------------------------
Generate client stubs from the official proto before running the subscriber:

1) Install tools
   pip install grpcio-tools

2) Fetch proto
   git clone https://github.com/forcedotcom/pub-sub-api.git _pubsub

3) Generate
   python -m grpc_tools.protoc -I _pubsub --python_out=googleads_sync/salesforce/grpc_stubs --grpc_python_out=googleads_sync/salesforce/grpc_stubs _pubsub/pubsub_api.proto

This creates:
  ads_sync/salesforce/grpc_stubs/pubsub_api_pb2.py
  ads_sync/salesforce/grpc_stubs/pubsub_api_pb2_grpc.py

Then run the subscriber task or management command below.
