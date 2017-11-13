REBAR = rebar3
.PHONY: compile test travis distclean coveralls

DIALYZER_APPS = kernel stdlib erts sasl eunit syntax_tools compiler crypto
DEP_DIR="_build/lib"

all: compile

travis: compile test xref dialyzer lint

test: eunit common_test cover

eunit:
	$(REBAR) eunit

common_test:
	mkdir -p priv/lager
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

distclean:
	rm -rf _build *.xml rebar3

coveralls:
	$(REBAR) as test do coveralls send || true
