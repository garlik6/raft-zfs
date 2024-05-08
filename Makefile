GOCMD=go build -v

all: helloworld

helloworld:
	$(GOCMD) -o example-helloworld

clean:
	@rm -f example-helloworld

.PHONY: helloworld clean
