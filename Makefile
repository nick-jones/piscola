gen-thrift:
	thrift --gen go -o internal/ thrift/service.thrift
	rm -r internal/gen-go/service/GoUnusedProtection__.go internal/gen-go/service/search-remote