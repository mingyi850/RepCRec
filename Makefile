all: build

build:
	go build -o repcrec cmd/repcrec.go

clean:
	rm -f repcrec

.PHONY: all build clean