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
-module(plumtree_test_object).

%% API
-export([context/1,
         value/1,
         values/1,
         modify/3,
         modify/4,
         is_stale/2,
         reconcile/2]).

-type test_object()     :: dvvset:clock().
-type test_context()    :: dvvset:vector().
-type test_value()      :: any().

%% @doc returns a single value. if the object holds more than one value an error is generated
%% @see values/2
-spec value(test_object()) -> test_value().
value(Object) ->
    [Value] = values(Object),
    Value.

%% @doc returns a list of values held in the object
-spec values(test_object()) -> [test_value()].
values(Object) ->
    [Value || {Value, _Ts} <- dvvset:values(Object)].

%% @doc returns the context (opaque causal history) for the given object
-spec context(test_object()) -> test_context().
context(Object) ->
    dvvset:join(Object).

%% @doc modifies a potentially existing object, setting its value and updating
%% the causual history. 
-spec modify(test_object() | undefined,
             test_value(),
             term()) -> test_object().
modify(undefined, Value, ServerId) ->
    modify(undefined, undefined, Value, ServerId);
modify(Object, Value, ServerId) ->
    modify(Object, context(Object), Value, ServerId).

-spec modify(test_object() | undefined,
             test_context(),
             test_value(),
             term()) -> test_object().
modify(undefined, _Context, Value, ServerId) ->
    %% Ignore the context since we dont have a value, its invalid if not
    %% empty anyways, so give it a valid one
    NewRecord = dvvset:new(timestamped_value(Value)),
    dvvset:update(NewRecord, ServerId);
modify(Existing, Context, Value, ServerId) ->
    InsertRec = dvvset:new(Context, timestamped_value(Value)),
    dvvset:update(InsertRec, Existing, ServerId).

%% @doc Determines if the given context (version vector) is causually newer than
%% an existing object. If the object missing or if the context does not represent
%% an ancestor of the current key, false is returned. Otherwise, when the context
%% does represent an ancestor of the existing object or the existing object itself,
%% true is returned
%% @private
is_stale(RemoteContext, Obj) ->
    LocalContext = context(Obj),
    %% returns true (stale) when local context is causally newer or equal to remote context
    descends(LocalContext, RemoteContext).

%% @doc Reconciles a remote object received during replication or anti-entropy
%% with a local object. If the remote object is an anscestor of or is equal to the local one
%% `false' is returned, otherwise the reconciled object is returned as the second
%% element of the two-tuple
reconcile(RemoteObj, undefined) ->
    {true, RemoteObj};
reconcile(undefined, _LocalObj) ->
    false;
reconcile(RemoteObj, LocalObj) ->
    Less  = dvvset:less(RemoteObj, LocalObj),
    Equal = dvvset:equal(RemoteObj, LocalObj),
    case not (Equal or Less) of
        false -> false;
        true ->
            LWW = fun ({_,TS1}, {_,TS2}) -> TS1 =< TS2 end,
            {true, dvvset:lww(LWW, dvvset:sync([LocalObj, RemoteObj]))}
    end.

%% @private
descends(_, []) ->
    true;
descends(Ca, Cb) ->
    [{NodeB, CtrB} | RestB] = Cb,
    case lists:keyfind(NodeB, 1, Ca) of
        false -> false;
        {_, CtrA} ->
            (CtrA >= CtrB) andalso descends(Ca, RestB)
    end.

%% @private
timestamped_value(Value) ->
    {Value, os:timestamp()}.

