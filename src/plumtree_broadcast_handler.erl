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
-module(plumtree_broadcast_handler).

%% Return a two-tuple of message id and payload from a given broadcast
-callback broadcast_data(any()) -> {any(), any()}.

%% Marshal a list of terms into opaque data.
-callback marshal(list(any())) -> {ok, any()}.

%% Unmarshall opaque data into a list of terms.
-callback unmarshal(any()) -> {ok, list(any())}.

%% Given the message id and payload, merge the message in the local state.
%% If the message has already been received return `false', otherwise return `true'
%% If a new message id is to be propagated after the merge return `{true, MessageId}`
-callback merge(any(), any()) -> boolean() | {true, any()}.

%% Return true if the message (given the message id) has already been received.
%% `false' otherwise
-callback is_stale(any()) -> boolean().

%% Return true if the first message argument (given the message id) is causally newer than the
%% second message argument (given message id as well)
%% `false' otherwise
-callback is_stale(any(), any()) -> boolean().

%% Return the message associated with the given message id. In some cases a message
%% has already been sent with information that subsumes the message associated with the given
%% message id. In this case, `stale' is returned.
-callback graft(any()) -> stale | {ok, any()} | {error, any()}.

%% Trigger an exchange between the local handler and the handler on the given node.
%% How the exchange is performed is not defined but it should be performed as a background
%% process and ensure that it delivers any messages missing on either the local or remote node.
%% The exchange does not need to account for messages in-flight when it is started or broadcast
%% during its operation. These can be taken care of in future exchanges.
-callback exchange(node()) -> {ok, pid()} | {error, term()}.
