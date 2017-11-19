%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Helium Systems, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(plumtree_SUITE).

-export([
         %% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0
        ]).

%% tests
-compile([export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").

-define(APP, plumtree).
-define(CLIENT_NUMBER, 3).
-define(PEER_PORT, 9000).

%% ===================================================================
%% common_test callbacks
%% ===================================================================

init_per_suite(_Config) ->
    _Config.

end_per_suite(_Config) ->
    _Config.

init_per_group(default, Config) ->
    Config;
init_per_group(hyparview, Config) ->
    [{partisan_peer_service_manager,
      partisan_hyparview_peer_service_manager}] ++ Config;
init_per_group(_, _Config) ->
    _Config.

end_per_group(_, _Config) ->
    ok.

init_per_testcase(Case, Config) ->
    ct:pal("Beginning test case ~p", [Case]),
    [{hash, erlang:phash2({Case, Config})}|Config].

end_per_testcase(Case, _Config) ->
    ct:pal("Ending test case ~p", [Case]),
    ok.

all() ->
    [{group, default, [shuffle]},
     {group, hyparview, [shuffle]}].

groups() ->
    [{default, [],
      [membership_simple_test,
       membership_high_client_test,
       broadcast_simple_test,
       broadcast_high_client_test]},
     {hyparview, [],
      [membership_simple_test,
       membership_high_client_test,
       broadcast_simple_test, 
       broadcast_simple_multiple_processes_test, 
       broadcast_high_active_test,
       broadcast_low_active_test,
       broadcast_high_client_test,
       broadcast_high_client_high_active_test,
       broadcast_high_client_high_active_multiple_processes_test]}].

broadcast_simple_test(Config) ->
    broadcast_test(Config).

broadcast_simple_multiple_processes_test(Config) ->
    broadcast_test([{n_broadcast_process, 5}] ++ Config).

broadcast_high_client_test(Config) ->
    broadcast_test([{n_clients, 11}] ++ Config).

broadcast_high_active_test(Config) ->
    broadcast_test([{max_active_size, 9},
                    {min_active_size, 9}] ++ Config).

broadcast_low_active_test(Config) ->
    broadcast_test([{max_active_size, 6},
                    {min_active_size, 6}] ++ Config).

broadcast_high_client_high_active_test(Config) ->
    broadcast_test([{max_active_size, 9},
                    {min_active_size, 9},
                    {n_clients, 11}] ++ Config).

broadcast_high_client_high_active_multiple_processes_test(Config) ->
    broadcast_test([{max_active_size, 6},
                    {min_active_size, 6},
                    {n_clients, 11},
                    {n_broadcast_process, 10}] ++ Config).

broadcast_high_client_low_active_test(Config) ->
    broadcast_test([{max_active_size, 6},
                    {min_active_size, 6},
                    {n_clients, 11}] ++ Config).

broadcast_partition_test(Config) ->
    broadcast_test([{max_active_size, 6},
                    {n_clients, 11},
                    {partition, true}] ++ Config).

membership_simple_test(Config) ->
    membership_test(Config).

membership_high_client_test(Config) ->
    membership_test([{n_clients, 11}] ++ Config).

%% @private
membership_test(Config) ->
    %% Use the default peer service manager.
    Manager = proplists:get_value(partisan_peer_service_manager,
                                  Config, partisan_default_peer_service_manager),
    NServers = proplists:get_value(n_servers, Config, 1),
    NClients = proplists:get_value(n_clients, Config, ?CLIENT_NUMBER),
    % Partition = proplists:get_value(partition, Config, false),
    MaxActiveSize = proplists:get_value(max_active_size, Config, 6),
    MinActiveSize = proplists:get_value(min_active_size, Config, 3),
    NBroadcastProcess = proplists:get_value(n_broadcast_process, Config, 1),

    %% Specify servers.
    Servers = node_list(NServers, "server", Config),

    %% Specify clients.
    Clients = node_list(NClients, "client", Config),

    %% Start nodes.
    Nodes = start(membership_test, Config,
                  [{partisan_peer_service_manager, Manager},
                   {max_active_size, MaxActiveSize},
                   {min_active_size, MinActiveSize},
                   {servers, Servers},
                   {clients, Clients},
                   {n_broadcast_process, NBroadcastProcess}]),

    %% Pause for clustering, allow one second per node
    timer:sleep(1000 * (NServers + NClients)),

    %% check membership after cluster
    check_connected_property(Nodes),

    BroadcastRounds1 = rand_compat:uniform(100),
    ct:pal("now doing ~p rounds of broadcast",
           [BroadcastRounds1]),
    %% do several rounds of broadcast from random nodes, then wait a bit for propagation
    lists:foreach(fun(_) ->
                    {_, Node} = plumtree_test_utils:select_random(Nodes),
                    ok = rpc:call(Node,
                                  plumtree_test_broadcast_handler, put,
                                  [k, rand_compat:uniform()])
                  end, lists:seq(1, BroadcastRounds1)),
    %% allow 100ms per broadcast to settle
    timer:sleep(100 * BroadcastRounds1),

    %% check membership after broadcast
    check_connected_property(Nodes, [{push_list, [eager, lazy]}]),
    
    %% now inject partitions in the broadcast tree until the graph is no longer connected
    
    %% do some rounds of broadcast in order to repair the tree
    BroadcastRounds2 = rand_compat:uniform(100),
    lists:foreach(fun(_) ->
                    {_, Node} = plumtree_test_utils:select_random(Nodes),
                    ok = rpc:call(Node,
                                  plumtree_test_broadcast_handler, put,
                                  [k, rand_compat:uniform()])
                  end, lists:seq(1, BroadcastRounds2)),
    %% allow 100ms per broadcast to settle
    timer:sleep(100 * BroadcastRounds1),

    stop(Nodes),
    ok.

%% @private
broadcast_test(Config) ->
    %% Use the default peer service manager.
    Manager = proplists:get_value(partisan_peer_service_manager,
                                  Config, partisan_default_peer_service_manager),
    NServers = proplists:get_value(n_servers, Config, 1),
    NClients = proplists:get_value(n_clients, Config, ?CLIENT_NUMBER),
    Partition = proplists:get_value(partition, Config, false),
    MaxActiveSize = proplists:get_value(max_active_size, Config, 6),
    MinActiveSize = proplists:get_value(min_active_size, Config, 3),
    NBroadcastProcess = proplists:get_value(n_broadcast_process, Config, 1),

    %% Specify servers.
    Servers = node_list(NServers, "server", Config),

    %% Specify clients.
    Clients = node_list(NClients, "client", Config),

    %% Start nodes.
    Nodes = start(broadcast_test, Config,
                  [{partisan_peer_service_manager, Manager},
                   {max_active_size, MaxActiveSize},
                   {min_active_size, MinActiveSize},
                   {servers, Servers},
                   {clients, Clients},
                   {n_broadcast_process, NBroadcastProcess}]),

    %% Pause for clustering, allow one second per node
    timer:sleep(1000 * (NServers + NClients)),

    %% check membership after cluster
    check_connected_property(Nodes),

    {ok, Reference} = maybe_partition(Partition, Manager, Nodes),
    maybe_resolve_partition(Partition, Reference, Manager, Nodes),

    %% do several rounds of broadcast from random nodes, then wait a bit for propagation
    BroadcastRounds1 = rand_compat:uniform(100),
    ct:pal("now doing ~p rounds of broadcast",
           [BroadcastRounds1]),
    lists:foreach(fun(_) ->
                    {_, Node} = plumtree_test_utils:select_random(Nodes),
                    %% generate a Key
                    Key = list_to_atom("k" ++
                                       integer_to_list(rand_compat:uniform(10000))),
                    %% shard broadcasts per key
                    Hash = erlang:phash2(Key, NBroadcastProcess) + 1,
                    Name = list_to_atom("test" ++ integer_to_list(Hash)),
                    ok = rpc:call(Node,
                                  plumtree_test_broadcast_handler, put,
                                  [Name, Key, rand_compat:uniform()])
                  end, lists:seq(1, BroadcastRounds1)),

    %% send out a control broadcast until it is seen at all nodes
    BroadcastSettleFun = fun() ->
                            %% select a random node to send out the control broadcast
                            Rand0 = rand_compat:uniform(),
                            {_, RandomNode} = plumtree_test_utils:select_random(Nodes),
                            ok = rpc:call(RandomNode,
                                          plumtree_test_broadcast_handler, put,
                                          [kcontrol, Rand0]),
                            ct:pal("requested node ~p to broadcast {kcontrol, ~p}",
                                   [RandomNode, Rand0]),
                            %% allow a small time window for propagation
                            timer:sleep(100),
                            %% now check that all others have received it
                            lists:foldl(fun(_, {false, _} = Acc) -> Acc;
                                            ({_, Node}, _) ->
                                            case rpc:call(Node, plumtree_test_broadcast_handler,
                                                          get, [kcontrol]) of
                                                {error, not_found} ->
                                                    {false, {not_found, Node}};
                                                {ok, NodeRand} when NodeRand =:= Rand0 -> true;
                                                {ok, NodeRand} ->
                                                    {false, {Node, Rand0, NodeRand}}
                                            end
                                         end, undefined, Nodes)
                         end,
    case wait_until(BroadcastSettleFun, 60 * 10, 100) of
        ok ->
            ok;
        {fail, {false, {not_found, Node}}} ->
            ct:fail("node ~p never got the control gossip",
                   [Node]);
        {fail, {false, {Node, Expected0, Contains0}}} ->
            ct:fail("node ~p had control value ~p, expected ~p",
                    [Node, Contains0, Expected0])
    end,

    %% check membership after broadcast storm
    check_connected_property(Nodes, [{push_list, [eager, lazy]}]),

    %% do a final round of broadcast, also from a random node, which is the one we'll be checking
    Rand = rand_compat:uniform(),
    {_, RandomNode} = plumtree_test_utils:select_random(Nodes),
    ok = rpc:call(RandomNode,
                  plumtree_test_broadcast_handler, put,
                  [k, Rand]),
    ct:pal("requested node ~p to broadcast {k, ~p}",
           [RandomNode, Rand]),

    VerifyFun = fun(Node, Rand0) ->
                    case rpc:call(Node, plumtree_test_broadcast_handler, get, [k]) of
                        {error, not_found} ->
                            {false, not_found};
                        {ok, NodeRand} when NodeRand =:= Rand0 -> true;
                        {ok, NodeRand} ->
                            {false, {Node, Rand0, NodeRand}}
                    end
                end,
    %% now check that the gossip has reached all nodes
    lists:foreach(fun({_, Node}) ->
                    VerifyBroadcastFun = fun() ->
                                            VerifyFun(Node, Rand)
                                         end,
                    case wait_until(VerifyBroadcastFun, 60 * 2, 100) of
                        ok ->
                            ok;
                        {fail, {false, not_found}} ->
                            ct:fail("node ~p never got the gossip",
                                   [Node]);
                        {fail, {false, {Node, Expected, Contains}}} ->
                            ct:fail("node ~p had value ~p, expected ~p",
                                    [Node, Contains, Expected])
                    end
                  end, Nodes),

    stop(Nodes),
    ok.

% %% ===================================================================
% %% utility functions
% %% ===================================================================

%% @private
node_list(0, _Name, _Config) -> [];
node_list(N, Name, Config) ->
    [ list_to_atom(string:join([Name,
                                integer_to_list(?config(hash, Config)),
                                integer_to_list(X)],
                               "_")) ||
        X <- lists:seq(1, N) ].

%% @private
start(_Case, Config, Options) ->
    StartFun = fun() ->
                    Nodes = start(Config, Options),
                    case check_connected_property(Nodes,
                                                  [{fail, false},
                                                   {push_list, [eager]}]) of
                        true -> {true, Nodes};
                        false ->
                            stop(Nodes),
                            {false, node_graph_is_not_connected}
                    end
               end,
    % try a few times to ensure that we have a connected graph on start
    case wait_until(StartFun, 5, 1000) of
        {ok, Nodes} ->
            Nodes;
        {fail, {false, {not_found, Node}}} ->
            ct:fail("node ~p never got the control gossip",
                   [Node]);
        {fail, {false, {Node, Expected0, Contains0}}} ->
            ct:fail("node ~p had control value ~p, expected ~p",
                    [Node, Contains0, Expected0])
    end.

start(Config, Options) ->
    %% Launch distribution for the test runner.
    ct:pal("Launching Erlang distribution..."),

    os:cmd(os:find_executable("epmd") ++ " -daemon"),
    {ok, Hostname} = inet:gethostname(),
    case net_kernel:start([list_to_atom("runner@" ++ Hostname), shortnames]) of
        {ok, _} ->
            ok;
        {error, {already_started, _}} ->
            ok
    end,

    %% Determine what is then current running test case
    TestStatus = ct:get_status(),
    {_Suite, TestCase} = proplists:get_value(current, TestStatus),

    %% Load sasl.
    application:load(sasl),
    ok = application:set_env(sasl,
                             sasl_error_logger,
                             false),
    application:start(sasl),

    %% Load lager.
    {ok, _} = application:ensure_all_started(lager),

    Servers = proplists:get_value(servers, Options, []),
    Clients = proplists:get_value(clients, Options, []),

    NodeNames = lists:flatten(Servers ++ Clients),

    %% Start all nodes.
    InitializerFun = fun(Name) ->
                            ct:pal("Starting node: ~p", [Name]),

                            NodeConfig = [{monitor_master, true},
                                          {startup_functions, [{code, set_path, [codepath()]}]}],

                            case ct_slave:start(Name, NodeConfig) of
                                {ok, Node} ->
                                    {Name, Node};
                                Error ->
                                    ct:fail(Error)
                            end
                     end,
    Nodes = lists:map(InitializerFun, NodeNames),

    %% Load applications on all of the nodes.
    LoaderFun = fun({_Name, Node}) ->
                            ct:pal("Loading applications on node: ~p", [Node]),

                            PrivDir = code:priv_dir(?APP),
                            LogDir = filename:join([PrivDir, "lager", atom_to_list(TestCase), Node]),

                            %% Manually force sasl loading, and disable the logger.
                            ok = rpc:call(Node, application, load, [sasl]),
                            ok = rpc:call(Node, application, set_env,
                                          [sasl, sasl_error_logger, false]),
                            ok = rpc:call(Node, application, start, [sasl]),

                            ok = rpc:call(Node, application, load, [plumtree]),
                            ok = rpc:call(Node, application, load, [lager]),
                            ok = rpc:call(Node, application, set_env, [sasl,
                                                                       sasl_error_logger,
                                                                       false]),
                            ok = rpc:call(Node, application, set_env, [lager,
                                                                       log_root,
                                                                       LogDir])
                     end,
    lists:map(LoaderFun, Nodes),

    %% Configure settings.
    ConfigureFun = fun({Name, Node}) ->
            %% Configure the peer service.
            PeerService = proplists:get_value(partisan_peer_service_manager, Options),
            ct:pal("Setting peer service manager on node ~p to ~p", [Node, PeerService]),
            ok = rpc:call(Node, partisan_config, set,
                          [partisan_peer_service_manager, PeerService]),

            MaxActiveSize = proplists:get_value(max_active_size, Options, 5),
            ok = rpc:call(Node, partisan_config, set,
                          [max_active_size, MaxActiveSize]),

            ok = rpc:call(Node, partisan_config, set, [tls, ?config(tls, Config)]),

            ok = rpc:call(Node, partisan_config, set,
                          [peer_ip, {127,0,0,1}]),

            Servers = proplists:get_value(servers, Options, []),
            Clients = proplists:get_value(clients, Options, []),

            %% Configure servers.
            case lists:member(Name, Servers) of
                true ->
                    ok = rpc:call(Node, partisan_config, set, [tag, server]),
                    ok = rpc:call(Node, partisan_config, set, [tls_options, ?config(tls_server_opts, Config)]);
                false ->
                    ok
            end,

            %% Configure clients.
            case lists:member(Name, Clients) of
                true ->
                    ok = rpc:call(Node, partisan_config, set, [tag, client]),
                    ok = rpc:call(Node, partisan_config, set, [tls_options, ?config(tls_client_opts, Config)]);
                false ->
                    ok
            end,

            %% configure plumtree
            ok = rpc:call(Node, application, set_env, [plumtree, broadcast_mod, plumtree_test_broadcast_handler]),
            NBroadcastProcs = proplists:get_value(n_broadcast_process, Config, 1),
            ok = rpc:call(Node, application, set_env, [plumtree, n_broadcast_process, NBroadcastProcs]),
            %% reduce the broacast exchange period down to 1 second
            ok = rpc:call(Node, application, set_env, [plumtree, broadcast_exchange_timer, 1000]),
            %% initialize the test broadcast handler
            ok = rpc:call(Node, plumtree_test_broadcast_handler, start_link, [])
    end,
    lists:foreach(ConfigureFun, Nodes),

    ct:pal("Starting nodes."),

    StartFun = fun({_Name, Node}) ->
                        %% Start plumtree.
                        {ok, _} = rpc:call(Node, plumtree, start, []),
                        %% set debug log level for test run
                        ok = rpc:call(Node, lager, set_loglevel, [{lager_file_backend,"log/console.log"}, debug]),
                        %% start custom named worker on each node
                        lists:foreach(fun(N) ->
                                        {ok, _} = rpc:call(Node, plumtree, new,
                                                           [list_to_atom("test" ++
                                                                         integer_to_list(N))])
                                      end, lists:seq(1, proplists:get_value(n_broadcast_process, Config, 1)))
               end,
    lists:foreach(StartFun, Nodes),

    ct:pal("Clustering nodes."),
    lists:foreach(fun(Node) ->
                    cluster(Node, Nodes, Options)
                  end, Nodes),

    lists:foreach(fun({_, Node}) ->
                      {ok, Members} = rpc:call(Node,
                                               partisan_peer_service,
                                               members,
                                               []),
                      ct:pal("node ~p active view: ~p",
                             [Node, Members])
                  end, Nodes),

    ct:pal("partisan fully initialized, nodes: ~p",
           [Nodes]),
    Nodes.

%% @private
%%
%% We have to cluster each node with all other nodes to compute the
%% correct overlay: for instance, sometimes you'll want to establish a
%% client/server topology, which requires all nodes talk to every other
%% node to correctly compute the overlay.
%%
cluster({Name, _Node} = Myself, Nodes, Options) when is_list(Nodes) ->
    Manager = proplists:get_value(partisan_peer_service_manager, Options),

    Servers = proplists:get_value(servers, Options, []),
    Clients = proplists:get_value(clients, Options, []),

    AmIServer = lists:member(Name, Servers),
    AmIClient = lists:member(Name, Clients),

    OtherNodes = case Manager of
                     partisan_default_peer_service_manager ->
                         %% Omit just ourselves.
                         omit([Name], Nodes);
                     partisan_client_server_peer_service_manager ->
                         case {AmIServer, AmIClient} of
                             {true, false} ->
                                %% If I'm a server, I connect to both
                                %% clients and servers!
                                omit([Name], Nodes);
                             {false, true} ->
                                %% I'm a client, pick servers.
                                omit(Clients, Nodes);
                             {_, _} ->
                                omit([Name], Nodes)
                         end;
                     partisan_hyparview_peer_service_manager ->
                        case {AmIServer, AmIClient} of
                            {true, false} ->
                               %% If I'm a server, I connect to both
                               %% clients and servers!
                               omit([Name], Nodes);
                            {false, true} ->
                               %% I'm a client, pick servers.
                               omit(Clients, Nodes);
                            {_, _} ->
                               omit([Name], Nodes)
                        end
                 end,
    lists:map(fun(OtherNode) -> cluster(Myself, OtherNode) end, OtherNodes).
cluster({_, Node}, {_, OtherNode}) ->
    PeerPort = rpc:call(OtherNode,
                        partisan_config,
                        get,
                        [peer_port, ?PEER_PORT]),
    ct:pal("Joining node: ~p to ~p at port ~p", [Node, OtherNode, PeerPort]),
    ok = rpc:call(Node,
                  partisan_peer_service,
                  join,
                  [#{name => OtherNode,
                     listen_addrs => [#{ip => {127, 0, 0, 1}, port => PeerPort}],
                     parallelism => 1}]).

%% @private
codepath() ->
    lists:filter(fun filelib:is_dir/1, code:get_path()).

%% @private
omit(OmitNameList, Nodes0) ->
    FoldFun = fun({Name, _Node} = N, Nodes) ->
                    case lists:member(Name, OmitNameList) of
                        true ->
                            Nodes;
                        false ->
                            Nodes ++ [N]
                    end
              end,
    lists:foldl(FoldFun, [], Nodes0).

maybe_partition(false, _, _) -> {ok, undefined};
maybe_partition(_, Manager, Nodes) ->
    %% Inject a partition.
    {_, PNode} = hd(Nodes),
    PFullNode = rpc:call(PNode, Manager, myself, []),

    {ok, Reference} = rpc:call(PNode, Manager, inject_partition, [PFullNode, 1]),
    ct:pal("Partition generated: ~p", [Reference]),

    %% Verify partition.
    PartitionVerifyFun = fun({_Name, Node}) ->
        {ok, Partitions} = rpc:call(Node, Manager, partitions, []),
        ct:pal("Partitions for node ~p: ~p", [Node, Partitions]),
        {ok, ActiveSet} = rpc:call(Node, Manager, active, []),
        Active = sets:to_list(ActiveSet),
        ct:pal("Peers for node ~p: ~p", [Node, Active]),
        PartitionedPeers = [Peer || {_Reference, Peer} <- Partitions],
        case PartitionedPeers == Active of
            true ->
                ok;
            false ->
                ct:fail("Partitions incorrectly generated.")
        end
    end,
    lists:foreach(PartitionVerifyFun, Nodes),
    {ok, Reference}.

maybe_resolve_partition(false, _, _, _) -> ok;
maybe_resolve_partition(_, Reference, Manager, Nodes) ->
    %% Resolve partition.
    {_, PNode} = hd(Nodes),
    ok = rpc:call(PNode, Manager, resolve_partition, [Reference]),
    ct:pal("Partition resolved: ~p", [Reference]),

    timer:sleep(1000),

    %% Verify resolved partition.
    ResolveVerifyFun = fun({_Name, Node}) ->
        {ok, Partitions} = rpc:call(Node, Manager, partitions, []),
        ct:pal("Partitions for node ~p: ~p", [Node, Partitions]),
        case Partitions == [] of
            true ->
                ok;
            false ->
                ct:fail("Partitions incorrectly resolved.")
        end
    end,
    lists:foreach(ResolveVerifyFun, Nodes),
    ok.

%% @private
connect(G, N1, N2) ->
    %% Add vertex for neighboring node.
    digraph:add_vertex(G, N1),
    % ct:pal("Adding vertex: ~p", [N1]),

    %% Add vertex for neighboring node.
    digraph:add_vertex(G, N2),
    % ct:pal("Adding vertex: ~p", [N2]),

    %% Add edge to that node.
    digraph:add_edge(G, N1, N2),
    % ct:pal("Adding edge from ~p to ~p", [N1, N2]),

    ok.

%% @private
check_connected_property(Nodes) ->
    check_connected_property(Nodes,
                             [{fail, true}, {push_list, [eager]}]).
                     
%% @private
check_connected_property(Nodes, Opts) ->
    PushList = proplists:get_value(push_list, Opts),
    %% Build the graphs.
    RootGraphs =
        lists:map(fun({_, Root}) ->
            %% build a graph for each node root
            Graph = digraph:new(),
            lists:foreach(fun({_, Node}) ->
                {Eagers, Lazys} = rpc:call(Node, plumtree_broadcast, debug_get_peers,
                                          [Node, Root]),
                ct:pal("node ~p peers rooted at ~p, eager: ~p, lazy: ~p",
                       [Node, Root, Eagers, Lazys]),
                %% Add vertexes and edges, for both eager and lazy lists
                case lists:member(eager, PushList) of
                    true ->
                        [connect(Graph, Node, N) || N <- Eagers];
                    false -> ok
                end,
                case lists:member(lazy, PushList) of
                    true ->
                        [connect(Graph, Node, N) || N <- Lazys];
                    false -> ok
                end
            end, Nodes),
            {Root, Graph}
        end, Nodes),
    %% Verify connectedness.
    Results =
        lists:map(fun({Root, Graph}) ->
                        lists:foreach(fun({_, N}) ->
                            Path = digraph:get_short_path(Graph, Root, N),
                            case Path of
                                false ->
                                    case proplists:get_value(fail, Opts, true) of
                                        true ->
                                            ct:fail("Graph is not connected, unable to find route between ~p and ~p",
                                                   [Root, N]),
                                            ok;
                                        false ->
                                            error
                                    end;
                                _ ->
                                    ct:pal("path found between ~p and ~p: ~p",
                                           [Root, N, Path]),
                                    ok
                            end
                        end, Nodes)
                  end, RootGraphs),
    not lists:member(error, Results).

%% @private
stop(Nodes) ->
    StopFun = fun({Name, _Node}) ->
        case ct_slave:stop(Name) of
            {ok, _} ->
                ok;
            Error ->
                ct:fail(Error)
        end
    end,
    lists:map(StopFun, Nodes),
    ok.

%% @private
wait_until(Fun, Retry, Delay) when Retry > 0 ->
    Res = Fun(),
    case Res of
        true ->
            ok;
        {true, Ret} ->
            {ok, Ret};
        _ when Retry == 1 ->
            {fail, Res};
        _ ->
            timer:sleep(Delay),
            wait_until(Fun, Retry-1, Delay)
    end.

%% @private
random_key() ->
    random_key(1000000).

random_key(N) ->
    list_to_atom("k_" ++
                 integer_to_list(rand_compat:uniform(N))).

broadcast_process_shard(K, NShards) ->
    list_to_atom("test" ++
                 integer_to_list(erlang:phash2(K, NShards) + 1)).
