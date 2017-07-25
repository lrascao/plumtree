REBAR = rebar3
.PHONY: deps compile rel test

DIALYZER_APPS = kernel stdlib erts sasl eunit syntax_tools compiler crypto
DEP_DIR="_build/lib"

all: compile

test: eunit common_test cover

eunit:
	$(REBAR) eunit

common_test:
	$(REBAR) ct

cover:
	$(REBAR) cover

lint:
	${REBAR} as lint lint

compile:
	$(REBAR) compile

stage:
	$(REBAR) release -d

xref:
	$(REBAR) xref

dialyzer:
	$(REBAR) dialyzer
