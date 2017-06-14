all: build

build: build-version build-proxy build-config build-collector

build-version:
	@bash genver.sh

build-proxy:
	go build -o bin/codis-proxy ./cmd/proxy

build-config:
	go build -o bin/codis-config ./cmd/cconfig
	@rm -rf bin/assets && cp -r cmd/cconfig/assets bin/

build-collector:
	go build -o bin/codis-collector ./cmd/collector

clean:
	@rm -rf bin
	@rm -f *.rdb *.out *.log *.dump deploy.tar
	@rm -f extern/Dockerfile
	@rm -f sample/log/*.log sample/nohup.out
	@if [ -d test ]; then cd test && rm -f *.out *.log *.rdb; fi

distclean: clean
	@make --no-print-directory --quiet -C extern/redis-2.8.21 clean

gotest:
	go test ./pkg/... ./cmd/...
