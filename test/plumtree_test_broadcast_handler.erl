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
-module(plumtree_test_broadcast_handler).

-behaviour(plumtree_broadcast_handler).
-behaviour(gen_server).

-define(SERVER, ?MODULE).

%% plumtree_broadcast_handler callbacks
-export([broadcast_data/1,
         merge/2,
         is_stale/1,
         graft/1,
         exchange/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%% API
-export([start_link/0,
         get/1,
         put/2]).

-record(state, {}).
-type state() :: #state{}.

-spec start_link() -> ok.
start_link() ->
    {ok, _} = gen_server:start_link({local, ?SERVER}, ?MODULE,
                                    [], []),
    ok.

-spec get(Key :: any()) -> {error, not_found} | {ok, any()}.
get(Key) ->
    case dbread(Key) of
        undefined -> {error, not_found};
        Obj ->
            {ok, plumtree_test_object:value(Obj)}
    end.

-spec put(Key :: any(),
          Value :: any()) -> ok.
put(Key, Value) ->
    Existing = dbread(Key),
    UpdatedObj = plumtree_test_object:modify(Existing, Value, this_server_id()),
    dbwrite(Key, UpdatedObj),
    plumtree_broadcast:broadcast({Key, UpdatedObj}, plumtree_test_broadcast_handler),
    ok.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
-spec init([[any()], ...]) -> {ok, state()}.
init([]) ->
    ?MODULE = ets:new(?MODULE, [named_table, set, public,
                                {keypos, 1},
                                {read_concurrency, true}]),
    {ok, #state{}}.

%% @private
-spec handle_call(term(), {pid(), term()}, state()) -> {reply, term(), state()}.
handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

%% @private
-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
-spec handle_info({'DOWN', _, 'process', _, _}, state()) ->
    {noreply, state()}.
handle_info({'DOWN', _Ref, process, _Pid, _Reason}, State) ->
    {noreply, State}.

%% @private
-spec terminate(term(), state()) -> term().
terminate(_Reason, _State) ->
    ok.

%% @private
-spec code_change(term() | {down, term()}, state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% plumtree_test_broadcast_handler callbacks
%%%===================================================================

%% Return a two-tuple of message id and payload from a given broadcast
-spec broadcast_data(any()) -> {any(), any()}.
broadcast_data({Key, Object}) ->
    MsgId = {Key, plumtree_test_object:context(Object)},
    lager:info("broadcast_data(~p), msg id: ~p",
               [Object, MsgId]),
    {MsgId, Object}.

%% Given the message id and payload, merge the message in the local state.
%% If the message has already been received return `false', otherwise return `true'
-spec merge(any(), any()) -> boolean() | {true, any()}.
merge({Key, _Context} = MsgId, RemoteObj) ->
    Existing = dbread(Key),
    lager:info("merge msg id ~p, remote object: ~p, existing object: ~p",
              [MsgId, RemoteObj, Existing]),
    case plumtree_test_object:reconcile(RemoteObj, Existing) of
        false -> false;
        {true, Reconciled} ->
            lager:info("merge object has ben reconciled to ~p",
                      [Reconciled]),
            dbwrite(Key, Reconciled),
            {true, {Key, plumtree_test_object:context(Reconciled)}}
    end.

%% Return true if the message (given the message id) has already been received.
%% `false' otherwise
-spec is_stale(any()) -> boolean().
is_stale({Key, Context}) ->
    case dbread(Key) of
        undefined -> false;
        Existing ->
            plumtree_test_object:is_stale(Context, Existing)
    end.

%% Return the message associated with the given message id. In some cases a message
%% has already been sent with information that subsumes the message associated with the given
%% message id. In this case, `stale' is returned.
-spec graft(any()) -> stale | {ok, any()} | {error, any()}.
graft({Key, Context}) ->
    case dbread(Key) of
        undefined ->
            %% this *really* should not happen
            lager:alert("unable to graft key ~p, could not find it",
                        [Key]),
            {error, not_found};
        Object ->
            LocalContext = plumtree_test_object:context(Object),
            case LocalContext =:= Context of
                true -> {ok, Object};
                false ->
                    lager:info("graft({~p, ~p}), context provided does not match local context ~p",
                               [Key, Context, LocalContext]),
                    stale
            end
    end.

%% Trigger an exchange between the local handler and the handler on the given node.
%% How the exchange is performed is not defined but it should be performed as a background
%% process and ensure that it delivers any messages missing on either the local or remote node.
%% The exchange does not need to account for messages in-flight when it is started or broadcast
%% during its operation. These can be taken care of in future exchanges.
-spec exchange(node()) -> {ok, pid()} | {error, term()}.
exchange(_Node) ->
    {ok, self()}.

%% @private
-spec dbread(Key :: any()) -> any() | undefined.
dbread(Key) ->
    case ets:lookup(?MODULE, Key) of
        [{Key, Object}] ->
            Object;
        _ ->
            undefined
    end.

%% @private
-spec dbwrite(Key :: any(),
              Value :: any()) -> any().
dbwrite(Key, Object) ->
    ets:insert(?MODULE, {Key, Object}),
    Object.

%% @private
this_server_id() -> node().

