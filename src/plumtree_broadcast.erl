%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
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
-module(plumtree_broadcast).

-behaviour(gen_server).

%% API
-export([start_link/0,
         start_link/5,
         broadcast/1,
         update/1,
         broadcast_members/0,
         broadcast_members/1,
         exchanges/0,
         exchanges/1,
         cancel_exchanges/1]).

%% Debug API
-export([debug_get_peers/2,
         debug_get_peers/3,
         debug_get_tree/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("plumtree.hrl").

-define(SERVER, ?MODULE).

-type nodename()        :: any().
-type message_id()      :: any().
-type message_round()   :: non_neg_integer().
-type outstanding()     :: {message_id(), message_round(), nodename()}.
-type exchange()        :: {node(), reference(), pid()}.
-type exchanges()       :: [exchange()].

-record(state, {
          %% `Mod' passed is responsible for handling the message on remote
          %% nodes as well as providing some other information both locally and on other nodes.
          %% `Mod' must be loaded on all members of the clusters and implement the
          %% `riak_core_broadcast_handler' behaviour.
          mod :: atom(),

          %% Initially trees rooted at each node are the same.
          %% Portions of that tree belonging to this node are
          %% shared in this set.
          common_eagers :: ordsets:ordset(nodename()) | undefined,

          %% Initially trees rooted at each node share the same lazy links.
          %% Typically this set will contain a single element. However, it may
          %% contain more in large clusters and may be empty for clusters with
          %% less than three nodes.
          common_lazys  :: ordsets:ordset(nodename()) | undefined,

          %% A mapping of sender node (root of each broadcast tree)
          %% to this node's portion of the tree. Elements are
          %% added to this structure as messages rooted at a node
          %% propogate to this node. Nodes that are never the
          %% root of a message will never have a key added to
          %% `eager_sets'
          eager_sets    :: [{nodename(), ordsets:ordset(nodename())}] | undefined,

          %% A Mapping of sender node (root of each spanning tree)
          %% to this node's set of lazy peers. Elements are added
          %% to this structure as messages rooted at a node
          %% propogate to this node. Nodes that are never the root
          %% of a message will never have a key added to `lazy_sets'
          lazy_sets     :: [{nodename(), ordsets:ordset(nodename())}] | undefined,

          %% Lazy messages that have not been acked. Messages are added to
          %% this set when a node is sent a lazy message (or when it should be
          %% sent one sometime in the future). Messages are removed when the lazy
          %% pushes are acknowleged via graft or ignores. Entries are keyed by their
          %% destination
          outstanding   :: [{nodename(), outstanding()}],

          %% List of outstanding exchanges
          exchanges     :: exchanges(),

          %% Set of all known members. Used to determine
          %% which members have joined and left during a membership update
          all_members   :: ordsets:ordset(nodename()) | undefined,

          %% Lazy tick period in milliseconds. On every tick all outstanding
          %% lazy pushes are sent out
          lazy_tick_period :: non_neg_integer(),

          %% Exchange tick period in milliseconds that may or may not occur
          exchange_tick_period :: non_neg_integer()

         }).
-type state()           :: #state{}.

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Starts the broadcast server on this node. The initial membership list is
%% fetched from the ring. If the node is a singleton then the initial eager and lazy
%% sets are empty. If there are two nodes, each will be in the others eager set and the
%% lazy sets will be empty. When number of members is less than 5, each node will initially
%% have one other node in its eager set and lazy set. If there are more than five nodes
%% each node will have at most two other nodes in its eager set and one in its lazy set, initally.
%% In addition, after the broadcast server is started, a callback is registered with ring_events
%% to generate membership updates as the ring changes.
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    LazyTickPeriod = application:get_env(plumtree, lazy_tick_period,
                                         ?DEFAULT_LAZY_TICK_PERIOD),
    ExchangeTickPeriod = application:get_env(plumtree, exchange_tick_period,
                                             ?DEFAULT_EXCHANGE_TICK_PERIOD),
    PeerService = application:get_env(plumtree, peer_service, partisan_peer_service),
    {ok, Members} = PeerService:members(),
    plumtree_util:log(debug, "peer sampling service members: ~p", [Members]),
    %% the peer service has already sampled the members, we start off
    %% with pure gossip (ie. all members are in the eager push list and lazy
    %% list is empty)
    InitEagers = Members,
    InitLazys = [],
    {ok, Mod} = application:get_env(plumtree, broadcast_mod),
    Res = start_link(Members, InitEagers, InitLazys, Mod,
                     [{lazy_tick_period, LazyTickPeriod},
                      {exchange_tick_period, ExchangeTickPeriod}]),
    PeerService:add_sup_callback(fun ?MODULE:update/1),
    Res.

%% @doc Starts the broadcast server on this node. `InitMembers' must be a list
%% of all members known to this node when starting the broadcast server.
%% `InitEagers' are the initial peers of this node for all broadcast trees.
%% `InitLazys' is a list of random peers not in `InitEagers' that will be used
%% as the initial lazy peer shared by all trees for this node. If the number
%% of nodes in the cluster is less than 3, `InitLazys' should be an empty list.
%% `InitEagers' and `InitLazys' must also be subsets of `InitMembers'. `Mods' is
%% a list of modules that may be handlers for broadcasted messages. All modules in
%% `Mod' should implement the `plumtree_broadcast_handler' behaviour.
%% `Opts' is a proplist with the following possible options:
%%      Flush all outstanding lazy pushes period (in milliseconds)
%%          {`lazy_tick_period', non_neg_integer()}
%%      Possibly perform an exchange period (in milliseconds)
%%          {`exchange_tick_period', non_neg_integer()}
%%
%% NOTE: When starting the server using start_link/2 no automatic membership update from
%% ring_events is registered. Use start_link/0.
-spec start_link([nodename()], [nodename()], [nodename()], module(),
                 proplists:proplist()) ->
    {ok, pid()} | ignore | {error, term()}.
start_link(InitMembers, InitEagers, InitLazys, Mod, Opts) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE,
                          [InitMembers, InitEagers, InitLazys, Mod, Opts], []).

%% @doc Broadcasts a message originating from this node. The message will be delivered to
%% each node at least once. The `Mod' passed is responsible for handling the message on remote
%% nodes as well as providing some other information both locally and and on other nodes.
%% `Mod' must be loaded on all members of the clusters and implement the
%% `riak_core_broadcast_handler' behaviour.
-spec broadcast(any()) -> ok.
broadcast(Broadcast) ->
    gen_server:cast(?SERVER, {broadcast, [Broadcast]}).

%% @doc Notifies broadcast server of membership update
update(LocalState0) ->
    PeerService = application:get_env(plumtree,
                                      peer_service,
                                      partisan_peer_service),
    LocalState = PeerService:decode(LocalState0),
    % lager:info("Update triggered with: ~p", [LocalState]),
    gen_server:cast(?SERVER, {update, LocalState}).

%% @doc Returns the broadcast servers view of full cluster membership.
%% Wait indefinitely for a response is returned from the process
-spec broadcast_members() -> ordsets:ordset(nodename()).
broadcast_members() ->
    broadcast_members(infinity).

%% @doc Returns the broadcast servers view of full cluster membership.
%% Waits `Timeout' ms for a response from the server
-spec broadcast_members(infinity | pos_integer()) -> ordsets:ordset(nodename()).
broadcast_members(Timeout) ->
    gen_server:call(?SERVER, broadcast_members, Timeout).

%% @doc return a list of exchanges, started by broadcast on thisnode, that are running
-spec exchanges() -> exchanges().
exchanges() ->
    exchanges(myself()).

%% @doc returns a list of exchanges, started by broadcast on `Node', that are running
-spec exchanges(node()) -> exchanges().
exchanges(Node) ->
    gen_server:call({?SERVER, Node}, exchanges, infinity).

%% @doc cancel exchanges started by this node.
-spec cancel_exchanges(all              |
                       {peer, node()}   |
                       reference()      |
                       pid()) -> exchanges().
cancel_exchanges(WhichExchanges) ->
    gen_server:call(?SERVER, {cancel_exchanges, WhichExchanges}, infinity).

%%%===================================================================
%%% Debug API
%%%===================================================================

%% @doc return the peers for `Node' for the tree rooted at `Root'.
%% Wait indefinitely for a response is returned from the process
-spec debug_get_peers(node(), node()) -> {ordsets:ordset(node()), ordsets:ordset(node())}.
debug_get_peers(Node, Root) ->
    debug_get_peers(Node, Root, infinity).

%% @doc return the peers for `Node' for the tree rooted at `Root'.
%% Waits `Timeout' ms for a response from the server
-spec debug_get_peers(node(), node(), infinity | pos_integer()) ->
                             {ordsets:ordset(node()), ordsets:ordset(node())}.
debug_get_peers(Node, Root, Timeout) ->
    gen_server:call({?SERVER, Node}, {get_peers, Root}, Timeout).

%% @doc return peers for all `Nodes' for tree rooted at `Root'
%% Wait indefinitely for a response is returned from the process
-spec debug_get_tree(node(), [node()]) ->
                            [{node(), {ordsets:ordset(node()), ordsets:ordset(node())}}].
debug_get_tree(Root, Nodes) ->
    [begin
         Peers = try debug_get_peers(Node, Root)
                 catch _:_ -> down
                 end,
         {Node, Peers}
     end || Node <- Nodes].

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
-spec init([[any()], ...]) -> {ok, state()}.
init([AllMembers, InitEagers, InitLazys, Mod, Opts]) ->
    plumtree_util:log(debug, "init ~p peers, eager: ~p, lazy: ~p",
                      [AllMembers, InitEagers, InitLazys]),
    LazyTickPeriod = proplists:get_value(lazy_tick_period, Opts),
    ExchangeTickPeriod = proplists:get_value(exchange_tick_period, Opts),
    schedule_lazy_tick(LazyTickPeriod),
    schedule_exchange_tick(ExchangeTickPeriod),
    State1 = #state{ mod = Mod,
                     outstanding = orddict:new(),
                     exchanges=[],
                     lazy_tick_period = LazyTickPeriod,
                     exchange_tick_period = ExchangeTickPeriod},
    State2 = reset_peers(AllMembers, InitEagers, InitLazys, State1),
    {ok, State2}.

%% @private
-spec handle_call(term(), {pid(), term()}, state()) -> {reply, term(), state()}.
handle_call({get_peers, Root}, _From, State) ->
    EagerPeers = all_peers(Root, State#state.eager_sets, State#state.common_eagers),
    LazyPeers = all_peers(Root, State#state.lazy_sets, State#state.common_lazys),
    {reply, {EagerPeers, LazyPeers}, State};
handle_call(broadcast_members, _From, State=#state{all_members=AllMembers}) ->
    {reply, AllMembers, State};
handle_call(exchanges, _From, State=#state{exchanges=Exchanges}) ->
    {reply, Exchanges, State};
handle_call({cancel_exchanges, WhichExchanges}, _From, State) ->
    Cancelled = cancel_exchanges(WhichExchanges, State#state.exchanges),
    {reply, Cancelled, State}.

%% @private
-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast({broadcast, Messages},
            #state{mod = Mod} = State0) ->
    State = lists:foldl(fun(Broadcast, StateAcc) ->
                            {MessageId, Message} = Mod:broadcast_data(Broadcast),
                            plumtree_util:log(debug, "received {broadcast, ~p, Msg}",
                                              [MessageId]),
                            State = eager_push(MessageId, Message, StateAcc),
                            schedule_lazy_push(MessageId, State)
                        end, State0, Messages),
    {noreply, State};
handle_cast({broadcast, From, Messages},
            #state{mod = Mod} = State0) ->
    State = lists:foldl(fun({MessageId, Message, Round, Root}, StateAcc) ->
                            plumtree_util:log(debug, "received {broadcast, Msg, ~p, ~p, ~p} from ~p",
                                              [MessageId, Round, Root, From]),
                            Valid = Mod:merge(MessageId, Message),
                            handle_broadcast(Valid, MessageId, Message, Round,
                                             Root, From, StateAcc)
                        end, State0, Messages),

    {noreply, State};
handle_cast({prune, Root, From}, State0) ->
    plumtree_util:log(debug, "received ~p from ~p", [{prune, Root}, From]),
    State1 = case add_lazy(From, Root, State0) of
                 already_added -> State0;
                 State ->
                    plumtree_util:log(debug, "moving peer ~p from eager to lazy on tree rooted at ~p",
                                      [From, Root]),
                    State
             end,
    {noreply, State1};
handle_cast({i_have, From, Messages}, State0) ->
    plumtree_util:log(debug, "received ~p i_have messages from ~p",
                      [length(Messages), From]),
    %% process i_have messages in bulk and return the new state along
    %% with lists of ignored_i_have and graft messages to be sent back
    {IgnoredMsgs, GraftMsgs, State} = handle_ihaves(From, Messages, State0),
    %% send out bulk ignore_i_have and graft messages
    BulkMsg = case IgnoredMsgs of
                  [] -> [];
                  _ ->
                      plumtree_util:log(debug, "sending ~p ignored_i_have messages to ~p",
                                        [length(IgnoredMsgs), From]),
                      [{ignored_i_have, myself(), IgnoredMsgs}]
              end ++
              case GraftMsgs of
                  [] -> [];
                  _ ->
                      plumtree_util:log(debug, "sending ~p graft messages to ~p",
                                        [length(GraftMsgs), From]),
                      [{graft, myself(), GraftMsgs}]
              end,
    _ = send(BulkMsg, From),
    {noreply, State};
handle_cast({ignored_i_have, From, Messages}, State) ->
    plumtree_util:log(debug, "received ~p ignored_i_have messages from ~p",
                      [length(Messages), From]),
    State1 =
        lists:foldl(fun({MessageId, Round, Root}, Acc) ->
                        ack_outstanding(MessageId, Round, Root, From, Acc)
                    end, State, Messages),
    {noreply, State1};
handle_cast({graft, From, Messages}, State) ->
    plumtree_util:log(debug, "received ~p graft messages from ~p",
                      [length(Messages), From]),
    {BroadcastMsgs, State1} = handle_grafts(From, Messages, State),
    _ = send({broadcast, myself(), BroadcastMsgs}, From),
    {noreply, State1};
handle_cast({update, Members}, State=#state{all_members=BroadcastMembers,
                                            common_eagers=EagerPeers0,
                                            common_lazys=LazyPeers}) ->
    plumtree_util:log(debug, "received ~p", [{update, Members}]),
    CurrentMembers = ordsets:from_list(Members),
    New = ordsets:subtract(CurrentMembers, BroadcastMembers),
    Removed = ordsets:subtract(BroadcastMembers, CurrentMembers),
    plumtree_util:log(debug, "    new members: ~p", [ordsets:to_list(New)]),
    plumtree_util:log(debug, "    removed members: ~p", [ordsets:to_list(Removed)]),
    State1 = case ordsets:size(New) > 0 of
                 false ->
                     State;
                 true ->
                     %% as per the paper (page 9):
                     %% "When a new member is detected, it is simply added to the set
                     %%  of eagerPushPeers"
                     EagerPeers = ordsets:union(EagerPeers0, New),
                     plumtree_util:log(debug, "    new peers, eager: ~p, lazy: ~p",
                                       [EagerPeers, LazyPeers]),
                     reset_peers(CurrentMembers, EagerPeers, LazyPeers, State)
             end,
    State2 = neighbors_down(Removed, State1),
    {noreply, State2}.

%% @private
-spec handle_info('exchange_tick' | 'lazy_tick' | {'DOWN', _, 'process', _, _}, state()) ->
    {noreply, state()}.
handle_info(lazy_tick,
            #state{lazy_tick_period = Period} = State) ->
    _ = send_lazy(State),
    schedule_lazy_tick(Period),
    {noreply, State};
handle_info(exchange_tick,
            #state{exchange_tick_period = Period} = State) ->
    State1 = maybe_exchange(State),
    schedule_exchange_tick(Period),
    {noreply, State1};
handle_info({'DOWN', Ref, process, _Pid, _Reason}, State=#state{exchanges=Exchanges}) ->
    Exchanges1 = lists:keydelete(Ref, 3, Exchanges),
    {noreply, State#state{exchanges=Exchanges1}}.

%% @private
-spec terminate(term(), state()) -> term().
terminate(_Reason, _State) ->
    ok.

%% @private
-spec code_change(term() | {down, term()}, state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
handle_broadcast(false, _MessageId, _Message, _Round, Root, From, State0) -> %% stale msg
    %% remove sender from eager and set as lazy
    case add_lazy(From, Root, State0) of
        already_added -> State0;
        State ->
            plumtree_util:log(debug, "moving peer ~p from eager to lazy on tree rooted at ~p, requesting it to also do the same",
                              [From, Root]),
            _ = send({prune, Root, myself()}, From),
            State
    end;
%% The next clause is designed to allow the callback to override the message id
%% after the merge, suppose node A eager pushed a change to node B, node B would then lazy
%% push it to node C, at this point the message id being sent to C is the one that originated
%% from A. Concurrently B takes an update that subsumes the previous one, now node C receives the
%% lazy push and hasn't seen this message and asks B to graft it, if C now sends the message id
%% that it got from A, node B will not answer the graft since it is deemed stale.
%% With this extra clause the callback is able to return a new message id that resulted from
%% the merge and have that be propagated.
handle_broadcast({true, MessageId}, _OldMessageId, Message, Round, Root, From, State) ->
    handle_broadcast(true, MessageId, Message, Round, Root, From, State);
handle_broadcast(true, MessageId, Message, Round, Root, From, State0) -> %% valid msg
    %% remove sender from lazy and set as eager
    State1 = case add_eager(From, Root, State0) of
                 already_added -> State0;
                 State ->
                    plumtree_util:log(debug, "moving peer ~p from lazy to eager on tree rooted at ~p",
                                      [From, Root]),
                    State
             end,
    State2 = eager_push(MessageId, Message, Round+1, Root, From, State1),
    schedule_lazy_push(MessageId, Round+1, Root, From, State2).

%% take in a list of i_have messages originating from a peer,
%% process them and return a tuple of three things:
%% a list of ignored_i_have messages, a list of graft messages
%% and the new state
handle_ihaves(From, Messages, State0) ->
    lists:foldl(fun(IHaveMessage, {IgnoredMsgs0, GraftMsgs0, StateAcc}) ->
                    %% process the i_have message
                    Res = handle_ihave(IHaveMessage, From, StateAcc),
                    %% and based on the return add it either to the ignored or
                    %% graft msgs list
                    handle_ihave_reply(Res, IHaveMessage, IgnoredMsgs0, GraftMsgs0)
                end, {[], [], State0}, Messages).

handle_ihave_reply({ignored, State}, Message, IgnoredMsgs, GraftMsgs) ->
    {IgnoredMsgs ++ [Message], GraftMsgs, State};
handle_ihave_reply({graft, State}, Message, IgnoredMsgs, GraftMsgs) ->
    {IgnoredMsgs, GraftMsgs ++ [Message], State}.

handle_ihave({MessageId, Round, Root}, From,
             #state{mod = Mod} = State) ->
    Stale = Mod:is_stale(MessageId),
    handle_ihave(Stale, MessageId, Round, Root, From, State).

handle_ihave(true, _MessageId, _Round, _Root, _From, State) -> %% stale i_have
    {ignored, State};
handle_ihave(false, _MessageId, _Round, Root, From, State0) -> %% valid i_have
    %% TODO: don't graft immediately
    case add_eager(From, Root, State0) of
        already_added ->
            plumtree_util:log(debug, "graft requested from ~p",
                              [From]),
            {graft, State0};
        State ->
            plumtree_util:log(debug, "moving peer ~p from lazy to eager on tree rooted at ~p, graft requested from ~p",
                              [From, Root, From]),
            {graft, State}
    end.

handle_grafts(From, Messages, State) ->
    lists:foldl(fun(GraftMessage, {BroadcastMsgs0, StateAcc}) ->
                    Res = handle_graft(GraftMessage, From, StateAcc),
                    handle_graft_reply(Res, BroadcastMsgs0)
                end, {[], State}, Messages).

handle_graft({MessageId, Round, Root}, From,
             #state{mod = Mod} = State) ->
    Result = Mod:graft(MessageId),
    plumtree_util:log(debug, "graft(~p): ~p", [MessageId, Result]),
    handle_graft(Result, MessageId, Round, Root, From, State).

handle_graft_reply({broadcast, Message, State}, BroadcastMsgs) ->
    {BroadcastMsgs ++ [Message], State};
handle_graft_reply({stale, State}, BroadcastMsgs) ->
    {BroadcastMsgs, State};
handle_graft_reply({error, State}, BroadcastMsgs) ->
    {BroadcastMsgs, State}.

handle_graft(stale, MessageId, Round, Root, From, State) ->
    %% There has been a subsequent broadcast that is causally newer than this message
    %% according to Mod. We ack the outstanding message since the outstanding entry
    %% for the newer message exists
    {stale, ack_outstanding(MessageId, Round, Root, From, State)};
handle_graft({ok, Message}, MessageId, Round, Root, From, State0) ->
    plumtree_util:log(debug, "moving peer ~p from lazy to eager on tree rooted at ~p",
                      [From, Root]),
    %% ack outstanding lazy push since it originated a graft from the peer
    State = ack_outstanding(MessageId, Round, Root, From,
                            maybe_add_eager(From, Root, State0)),
    {broadcast, {MessageId, Message, Round, Root}, State};
handle_graft({error, Reason}, _MessageId, _Round, _Root, From, State) ->
    lager:error("unable to graft message from ~p. reason: ~p", [From, Reason]),
    {error, State}.

neighbors_down(Removed, State=#state{common_eagers=CommonEagers, eager_sets=EagerSets,
                                     common_lazys=CommonLazys, lazy_sets=LazySets,
                                     outstanding=Outstanding}) ->
    NewCommonEagers = ordsets:subtract(CommonEagers, Removed),
    NewCommonLazys  = ordsets:subtract(CommonLazys, Removed),
    %% TODO: once we have delayed grafting need to remove timers
    NewEagerSets = ordsets:from_list([{Root, ordsets:subtract(Existing, Removed)} ||
                                         {Root, Existing} <- ordsets:to_list(EagerSets)]),
    NewLazySets  = ordsets:from_list([{Root, ordsets:subtract(Existing, Removed)} ||
                                         {Root, Existing} <- ordsets:to_list(LazySets)]),
    %% delete outstanding messages to removed peers
    NewOutstanding = ordsets:fold(fun(RPeer, OutstandingAcc) ->
                                          orddict:erase(RPeer, OutstandingAcc)
                                  end,
                                  Outstanding, Removed),
    State#state{common_eagers=NewCommonEagers,
                common_lazys=NewCommonLazys,
                eager_sets=NewEagerSets,
                lazy_sets=NewLazySets,
                outstanding=NewOutstanding}.

eager_push(MessageId, Message, State) ->
    eager_push(MessageId, Message, 0, myself(), myself(), State).

eager_push(MessageId, Message, Round, Root, From, State) ->
    Peers = eager_peers(Root, From, State),
    eager_push(Peers, MessageId, Message, Round, Root, From, State).

eager_push([], _MessageId, _Message, _Round, _Root, _From, State) -> State;
eager_push(Peers, MessageId, Message, Round, Root, From, State) ->
    plumtree_util:log(debug, "eager push to peers ~p for tree rooted on ~p originating from ~p",
                      [Peers, Root, From]),
    _ = send({broadcast, myself(), [{MessageId, Message, Round, Root}]}, Peers),
    State.

schedule_lazy_push(MessageId, State) ->
    schedule_lazy_push(MessageId, 0, myself(), myself(), State).

schedule_lazy_push(MessageId, Round, Root, From, State) ->
    Peers = lazy_peers(Root, From, State),
    plumtree_util:log(debug, "scheduling lazy push to peers ~p: ~p",
               [Peers, {MessageId, Round, Root, From}]),
    add_all_outstanding(MessageId, Round, Root, Peers, State).

send_lazy(#state{outstanding=Outstanding}) ->
    [send_lazy(Peer, Messages) || {Peer, Messages} <- orddict:to_list(Outstanding)].

send_lazy(_Peer, []) -> ok;
send_lazy(Peer, Messages) ->
    plumtree_util:log(debug, "flushing ~p outstanding lazy pushes to peer ~p",
                      [ordsets:size(Messages), Peer]),
    send({i_have, myself(), Messages}, Peer).

maybe_exchange(State) ->
    Root = random_root(State),
    Peer = random_peer(Root, State),
    maybe_exchange(Peer, State).

maybe_exchange(undefined, State) ->
    State;
maybe_exchange(Peer, State=#state{exchanges=Exchanges}) ->
    %% limit the number of exchanges this node can start concurrently.
    %% the exchange must (currently?) implement any "inbound" concurrency limits
    ExchangeLimit = app_helper:get_env(plumtree, broadcast_start_exchange_limit, 1),
    BelowLimit = not (length(Exchanges) >= ExchangeLimit),
    case BelowLimit of
        true -> exchange(Peer, State);
        false -> State
    end.

exchange(Peer, State=#state{mod = Mod, exchanges=Exchanges}) ->
    case Mod:exchange(Peer) of
        {ok, Pid} ->
            plumtree_util:log(debug, "started ~p exchange with ~p (~p)", [Peer, Pid]),
            Ref = monitor(process, Pid),
            State#state{exchanges=[{Peer, Ref, Pid} | Exchanges]};
        {error, _Reason} ->
            State
    end.

cancel_exchanges(all, Exchanges) ->
    kill_exchanges(Exchanges);
cancel_exchanges(WhichProc, Exchanges) when is_reference(WhichProc) orelse
                                            is_pid(WhichProc) ->
    KeyPos = case is_reference(WhichProc) of
              true -> 3;
              false -> 4
          end,
    case lists:keyfind(WhichProc, KeyPos, Exchanges) of
        false -> [];
        Exchange ->
            kill_exchange(Exchange),
            [Exchange]
    end;
cancel_exchanges(Which, Exchanges) ->
    Filter = exchange_filter(Which),
    ToCancel = [Ex || Ex <- Exchanges, Filter(Ex)],
    kill_exchanges(ToCancel).

kill_exchanges(Exchanges) ->
    _ = [kill_exchange(Exchange) || Exchange <- Exchanges],
    Exchanges.

kill_exchange({_, _, ExchangePid}) ->
    exit(ExchangePid, cancel_exchange).

exchange_filter({peer, Peer}) ->
    fun({ExchangePeer, _, _}) ->
            Peer =:= ExchangePeer
    end.

%% picks random root uniformly
random_root(#state{all_members=Members}) ->
    random_other_node(Members).

%% picks random peer favoring peers not in eager or lazy set and ensuring
%% peer is not this node
random_peer(Root, State=#state{all_members=All}) ->
    Mode = application:get_env(plumtree, exchange_selection, optimized),
    Other = case Mode of
        %% Normal; randomly select a peer from the known membership at
        %% this node.
        normal ->
            ordsets:del_element(myself(), All);
        %% Optimized; attempt to find a peer that's not in the broadcast
        %% tree, to increase probability of selecting a lagging node.
        optimized ->
            Eagers = all_eager_peers(Root, State),
            Lazys  = all_lazy_peers(Root, State),
            Union  = ordsets:union([Eagers, Lazys]),
            ordsets:del_element(myself(), ordsets:subtract(All, Union))
    end,
    Selected = case ordsets:size(Other) of
        0 ->
            random_other_node(ordsets:del_element(myself(), All));
        _ ->
            random_other_node(Other)
    end,
    Selected.

%% picks random node from ordset
random_other_node(OrdSet) ->
    Size = ordsets:size(OrdSet),
    case Size of
        0 -> undefined;
        _ ->
            lists:nth(rand_compat:uniform(Size),
                     ordsets:to_list(OrdSet))
    end.

ack_outstanding(MessageId, Round, Root, From, State=#state{outstanding=All}) ->
    Existing = existing_outstanding(From, All),
    Updated = set_outstanding(From,
                              ordsets:del_element({MessageId, Round, Root}, Existing),
                              All),
    State#state{outstanding=Updated}.

add_all_outstanding(MessageId, Round, Root, Peers, State) ->
    lists:foldl(fun(Peer, SAcc) -> add_outstanding(MessageId, Round, Root, Peer, SAcc) end,
                State,
                ordsets:to_list(Peers)).

add_outstanding(MessageId, Round, Root, Peer, State=#state{outstanding=All}) ->
    Existing = existing_outstanding(Peer, All),
    Updated = set_outstanding(Peer,
                              ordsets:add_element({MessageId, Round, Root}, Existing),
                              All),
    State#state{outstanding=Updated}.

set_outstanding(Peer, Outstanding, All) ->
    case ordsets:size(Outstanding) of
        0 -> orddict:erase(Peer, All);
        _ -> orddict:store(Peer, Outstanding, All)
    end.

existing_outstanding(Peer, All) ->
    case orddict:find(Peer, All) of
        error -> ordsets:new();
        {ok, Outstanding} -> Outstanding
    end.

add_eager(From, Root, State) ->
    CurrentEagers = all_eager_peers(Root, State),
    maybe_update_peers({eager, CurrentEagers}, From, Root,
                       fun ordsets:add_element/2, fun ordsets:del_element/2, State).

maybe_add_eager(From, Root, State0) ->
    case add_eager(From, Root, State0) of
        already_added -> State0;
        NewState -> NewState
    end.

add_lazy(From, Root, State) ->
    CurrentLazys = all_lazy_peers(Root, State),
    maybe_update_peers({lazy, CurrentLazys}, From, Root,
                       fun ordsets:del_element/2, fun ordsets:add_element/2, State).

maybe_update_peers({_Type, Set} = TypeSet, From, Root,
                   EagerUpdate, LazyUpdate, State) ->
    case ordsets:is_element(From, Set) of
        true -> already_added;
        false ->
            OtherTypeSet = other_type_set(TypeSet, Root, State),
            update_peers(From, Root, [TypeSet, OtherTypeSet],
                         EagerUpdate, LazyUpdate, State)
    end.

other_type_set({eager, _}, Root, State) -> {lazy, all_lazy_peers(Root, State)};
other_type_set({lazy, _}, Root, State) -> {eager, all_eager_peers(Root, State)}.

update_peers(From, Root, PeerLists, EagerUpdate, LazyUpdate, State) ->
    {eager, Eagers} = lists:keyfind(eager, 1, PeerLists),
    {lazy, Lazys} = lists:keyfind(lazy, 1, PeerLists),
    NewEagers = EagerUpdate(From, Eagers),
    NewLazys  = LazyUpdate(From, Lazys),
    set_peers(Root, NewEagers, NewLazys, State).

set_peers(Root, Eagers, Lazys, State=#state{eager_sets=EagerSets, lazy_sets=LazySets}) ->
    NewEagers = orddict:store(Root, Eagers, EagerSets),
    NewLazys = orddict:store(Root, Lazys, LazySets),
    State#state{eager_sets=NewEagers, lazy_sets=NewLazys}.

all_eager_peers(Root, State) ->
    all_peers(Root, State#state.eager_sets, State#state.common_eagers).

all_lazy_peers(Root, State) ->
    all_peers(Root, State#state.lazy_sets, State#state.common_lazys).

eager_peers(Root, From, #state{eager_sets=EagerSets, common_eagers=CommonEagers}) ->
    all_filtered_peers(Root, From, EagerSets, CommonEagers).

lazy_peers(Root, From, #state{lazy_sets=LazySets, common_lazys=CommonLazys}) ->
    all_filtered_peers(Root, From, LazySets, CommonLazys).

all_filtered_peers(Root, From, Sets, Common) ->
    All = all_peers(Root, Sets, Common),
    ordsets:del_element(From, All).

all_peers(Root, Sets, Default) ->
    case orddict:find(Root, Sets) of
        error -> Default;
        {ok, Peers} -> Peers
    end.

%% allow either sending several messages to a peer,
%% the same message to several peers or one message
%% to a single peer
send(Msgs, Peer) when is_list(Msgs) ->
    [send(Msg, Peer) || Msg <- Msgs];
send(Msg, Peers) when is_list(Peers) ->
    [send(Msg, P) || P <- Peers];
send(Msg, P) ->
    PeerService = application:get_env(plumtree,
                                      peer_service,
                                      partisan_peer_service),
    PeerServiceManager = PeerService:manager(),
    ok = PeerServiceManager:forward_message(P, ?SERVER, Msg).
    %% TODO: add debug logging
    %% gen_server:cast({?SERVER, P}, Msg).

schedule_lazy_tick(Period) ->
    schedule_tick(lazy_tick, broadcast_lazy_timer, Period).

schedule_exchange_tick(Period) ->
    schedule_tick(exchange_tick, broadcast_exchange_timer, Period).

schedule_tick(Message, Timer, Default) ->
    TickMs = app_helper:get_env(plumtree, Timer, Default),
    erlang:send_after(TickMs, ?MODULE, Message).

reset_peers(AllMembers, EagerPeers, LazyPeers, State) ->
    State#state{
      common_eagers = ordsets:del_element(myself(), ordsets:from_list(EagerPeers)),
      common_lazys  = ordsets:del_element(myself(), ordsets:from_list(LazyPeers)),
      eager_sets    = orddict:new(),
      lazy_sets     = orddict:new(),
      all_members   = ordsets:from_list(AllMembers)
     }.

%% @private
myself() ->
    node().

