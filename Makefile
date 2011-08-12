REBAR:=rebar
APPSRC:=src/sentinerl.app.src
VERSION:=$(shell grep vsn $(APPSRC) | awk -F\" '{print $$2}')


.PHONY: all erl test clean doc 

all: erl

erl:
	$(REBAR) get-deps compile

test:
	@mkdir -p .eunit
	$(REBAR) skip_deps=true eunit

update:
	$(REBAR) update-deps

mr:
	$(REBAR) -C rebar.mr.config get-deps compile

test-mr:
	@mkdir -p .eunit
	$(REBAR) -C rebar.mr.config skip_deps=true eunit

install: mr
	./apps/mapreduce/install_jobs.sh -av $(HOST)

release: all
	$(REBAR) generate
	tar jcf rel/sentinerl_$(VERSION).tbz2 -C rel sentinerl

clean:
	$(REBAR) clean
	-rm -rvf deps ebin doc .eunit

doc:
	$(REBAR) doc

run:
	ERL_LIBS=deps erl -pa apps/*/ebin apps/*/include -name sentinerl@127.0.0.1 -config rel/files/app.config -eval "application:start(sasl), application:start(sentinerl)."
