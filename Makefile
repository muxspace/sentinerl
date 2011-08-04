REBAR:=rebar
APPSRC:=src/sentinerl.app.src
VERSION:=$(shell grep vsn $(APPSRC) | awk -F\" '{print $$2}')


.PHONY: all erl test clean doc 

all: erl

erl:
	$(REBAR) get-deps compile

test:
	cp rekon/erlang/*.erl src
	$(REBAR) get-deps compile
	@mkdir -p .eunit
	$(REBAR) skip_deps=true eunit

update:
	$(REBAR) update-deps

install:
	./rekon/install_jobs.sh -av $(HOST)

release: all
	$(REBAR) generate
	tar jcf rel/sentinerl_$(VERSION).tbz2 -C rel sentinerl

clean:
	$(REBAR) skip_deps=true clean
	-rm -rvf ebin doc .eunit

doc:
	$(REBAR) doc

run:
	ERL_LIBS=deps erl -pa apps/*/ebin apps/*/include -name sentinerl@127.0.0.1 -config rel/files/app.config -eval "application:start(sasl), application:start(sentinerl)."
