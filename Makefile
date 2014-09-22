HOST = 10.0.1.140

all: cass_top tests.sh assert.sh
	bash -n cass_top tests.sh

test: tests.sh
	./tests.sh $(HOST)

assert.sh: 
	wget -q -T 15 --no-check-certificate https://raw.githubusercontent.com/lehmannro/assert.sh/master/assert.sh
