all: build

build:
	go build -o repcrec cmd/repcrec.go

test:
	go test ./test/...

clean:
	rm -f repcrec

.PHONY: all build clean test