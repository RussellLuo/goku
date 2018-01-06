.PHONY: gen-pb

gen-pb:
	cd cmd/goku-proxy && ${GOPATH}/bin/protoc gokuproxy.proto --go_out=plugins=grpc:pb
	cd cmd/goku-proxy && ${GOPATH}/bin/protoc gokuproxy.proto --gohttp_out=pb_pkg_path=github.com/RussellLuo/goku/cmd/goku-proxy/pb:http

	cd cmd/goku-server && ${GOPATH}/bin/protoc gokuserver.proto --go_out=plugins=grpc:pb
	cd cmd/goku-server && ${GOPATH}/bin/protoc gokuserver.proto --gohttp_out=pb_pkg_path=github.com/RussellLuo/goku/cmd/goku-server/pb:http

	cd group && rm pb && ln -s ../cmd/goku-server/pb pb
