# usage: HOST=1.2.3.4 make

all: cass_top test

test: tests.sh assert.sh
	bash -n cass_top tests.sh
	./tests.sh $(HOST)

assert.sh:
	wget -q -T 15 --no-check-certificate https://raw.githubusercontent.com/lehmannro/assert.sh/master/assert.sh

