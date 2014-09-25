# usage: HOST=1.2.3.4 MOCK=[1] make

all: cass_top test

test: tests.sh assert.sh mock_nodetool
	bash -n cass_top tests.sh
	MOCK=$(MOCK) ./tests.sh $(HOST)

assert.sh:
	wget -q -T 15 --no-check-certificate https://raw.githubusercontent.com/lehmannro/assert.sh/master/assert.sh

