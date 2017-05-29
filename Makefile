REBAR = $(shell pwd)/rebar3
.PHONY: deps compile rel test

DIALYZER_APPS = kernel stdlib erts sasl eunit syntax_tools compiler crypto
DEP_DIR="_build/lib"

all: compile

include tools.mk

test: common_test cover

common_test:
	$(REBAR) ct

cover:
	$(REBAR) cover

lint:
	${REBAR} as lint lint

compile:
	$(REBAR) compile

rel:
	$(REBAR) release

stage:
	$(REBAR) release -d

dialyzer:
	$(REBAR) dialyzer
