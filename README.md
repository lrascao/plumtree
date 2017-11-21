Plumtree
=======================================================

[![Build Status](https://travis-ci.org/lrascao/plumtree.svg?branch=develop)](https://travis-ci.org/lrascao/plumtree)

[![Coverage Status](https://coveralls.io/repos/github/lrascao/plumtree/badge.svg?branch=develop)](https://coveralls.io/github/lrascao/plumtree?branch=develop)

Plumtree is an implementation of [Plumtree](http://www.gsd.inesc-id.pt/~ler/reports/srds07.pdf), the epidemic broadcast protocol.  It is based in [Lasp's]() implementation which in turn is based off [Riak Core](https://github.com/basho/riak_core). It uses [Partisan](https://github.com/lasp-lang/partisan) a peer sampling service which notably offers [Hyparview](http://asc.di.fct.unl.pt/~jleitao/pdf/dsn07-leitao.pdf), the Hybrid Partial view protocol.

More information on the plumtree protocol and it's history we encourage you to watch Jordan West's [RICON West 2013 talk](https://www.youtube.com/watch?v=s4cCUTPU8GI) and Joao Leitao & Jordan West's [RICON 2014 talk](https://www.youtube.com/watch?v=bo367a6ZAwM).

Credits due to the Basho team on the original implementation, Helium's extraction from Riak to a separate project and Lasp's integration with Partisan.

Build
-----

    $ make

Testing
-------

    $ make test
    $ make xref
    $ make dialyzer

Contributing
----

Contributions from the community are encouraged. This project follows the git-flow workflow. If you want to contribute:

* Fork this repository
* Make your changes and run the full test suite
 * Please include any additional tests for any additional code added
* Commit your changes and push them to your fork
* Open a pull request

We will review your changes, make appropriate suggestions and/or provide feedback, and merge your changes when ready.
