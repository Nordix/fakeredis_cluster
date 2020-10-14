-module(fakeredis_cluster_SUITE).

%% Test framework
-export([ init_per_suite/1
        , end_per_suite/1
        , all/0
        , suite/0
        ]).

%% Test cases
-export([ t_cluster_slots/1
        , t_start_masters_only/1
        , t_start_masters_with_single_replica/1
        , t_start_masters_with_2_replicas/1
        , t_moved_redirect/1
        , t_ask_redirect/1
        , t_script_handling/1
        , t_kill_node_on_command/1
        ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(NL, "\r\n").

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(fakeredis_cluster),
    Config.

end_per_suite(_Config) ->
    application:stop(fakeredis_cluster),
    ok.

all() -> [F || {F, _A} <- module_info(exports),
               case atom_to_list(F) of
                   "t_" ++ _ -> true;
                   _         -> false
               end].

suite() -> [{timetrap, {seconds, 5}}].

%% Test

t_cluster_slots(Config) when is_list(Config) ->
    %% Setup cluster and request CLUSTER SLOTS
    fakeredis_cluster:start_link([30001, 30002, 30003, 30004, 30005, 30006]),
    {ok, Sock} = gen_tcp:connect("localhost", 30001,
                                 [binary, {active, false}, {packet, 0}]),

    Data = ["*2", ?NL, "$7", ?NL, "cluster", ?NL, "$5", ?NL, "slots", ?NL],
    ok = gen_tcp:send(Sock, Data),
    {ok, _Data} = gen_tcp:recv(Sock, 0),

    %% Kill a fake Redis node
    fakeredis_cluster:kill_node(30001),
    ?assertMatch(ok, gen_tcp:send(Sock, Data)),
    ?assertMatch({error, closed}, gen_tcp:recv(Sock, 0)),

    %% Need to wait some time after kill_node/1 to be able
    %% to restart node and setup new server socket
    timer:sleep(100),

    %% Restart the node again
    fakeredis_cluster:restart_node(30001),
    {ok, Sock2} = gen_tcp:connect("localhost", 30001,
                                 [binary, {active, false}, {packet, 0}]),
    ok = gen_tcp:send(Sock2, Data),
    {ok, _Data} = gen_tcp:recv(Sock2, 0),

    %% Stop a fake Redis node
    fakeredis_cluster:stop_node(30001),
    ?assertMatch(ok, gen_tcp:send(Sock2, Data)),
    ?assertMatch({error, closed}, gen_tcp:recv(Sock2, 0)),

    %% Restart the node again
    fakeredis_cluster:restart_node(30001),
    {ok, Sock3} = gen_tcp:connect("localhost", 30001,
                                 [binary, {active , false}, {packet, 0}]),
    ok = gen_tcp:send(Sock3, Data),
    {ok, _Data} = gen_tcp:recv(Sock3, 0),
    ok = gen_tcp:close(Sock3),

    %% Check event log, mostly to check that the event log functionality works.
    timer:sleep(100),
    ?assertMatch([{Connection1, connect, _},
                  {Connection1, command, [<<"cluster">>, <<"slots">>]},
                  {Connection1, reply, _},
                  {Connection2, connect, _},
                  {Connection2, command, [<<"cluster">>, <<"slots">>]},
                  {Connection2, reply, _},
                  {Connection3, connect, _},
                  {Connection3, command, [<<"cluster">>, <<"slots">>]},
                  {Connection3, reply, _},
                  {Connection3, disconnect, normal}
                 ],
                 fakeredis_cluster:get_event_log()),
    ok = fakeredis_cluster:clear_event_log(),
    [] = fakeredis_cluster:get_event_log().

t_start_masters_only(Config) when is_list(Config) ->
    fakeredis_cluster:start_link([20010, 20020, 20030]),
    {ok, Sock} = gen_tcp:connect("localhost", 20010,
                                 [binary, {active, false}, {packet, 0}]),

    Data = ["*2", ?NL, "$7", ?NL, "cluster", ?NL, "$5", ?NL, "slots", ?NL],
    ok = gen_tcp:send(Sock, Data),
    {ok, _Data} = gen_tcp:recv(Sock, 0),
    ok = gen_tcp:close(Sock).

t_start_masters_with_single_replica(Config) when is_list(Config) ->
    fakeredis_cluster:start_link([{20010, 20011},
                                  {20020, 20021},
                                  {20030, 20031}]),

    {ok, Sock} = gen_tcp:connect("localhost", 20010,
                                 [binary, {active, false}, {packet, 0}]),

    Data = ["*2", ?NL, "$7", ?NL, "cluster", ?NL, "$5", ?NL, "slots", ?NL],
    ok = gen_tcp:send(Sock, Data),
    {ok, _Data} = gen_tcp:recv(Sock, 0),
    ok = gen_tcp:close(Sock).

t_start_masters_with_2_replicas(Config) when is_list(Config) ->
    fakeredis_cluster:start_link([{20010, 20011, 20012},
                                  {20020, 20021, 20022},
                                  {20030, 20031, 20031}]),

    {ok, Sock} = gen_tcp:connect("localhost", 20010,
                                 [binary, {active, false}, {packet, 0}]),

    Data = ["*2", ?NL, "$7", ?NL, "cluster", ?NL, "$5", ?NL, "slots", ?NL],
    ok = gen_tcp:send(Sock, Data),
    {ok, _Data} = gen_tcp:recv(Sock, 0),
    ok = gen_tcp:close(Sock).

t_moved_redirect(Config) when is_list(Config) ->
    ClusterPorts = [20040, 20041, 20042],
    fakeredis_cluster:start_link(ClusterPorts),

    Key  = <<"foo">>,
    Slot = 12182 = fakeredis_hash:hash(Key),
    {Addr, SlotPort} = fakeredis_cluster:get_node_by_slot(Slot),
    [DifferentPort | _] = [P || P <- ClusterPorts,
                                P =/= SlotPort],

    %% Check that the results of
    %% fakeredis_cluster:get_redirect_by_key/1 and
    %% fakeredis_cluster:get_node_by_slot/1 are consistent.
    ?assertEqual({moved, Slot, Addr, SlotPort},
                 fakeredis_cluster:get_redirect_by_key(<<"foo">>)),

    %% Check what's returned by the correct node and a different node respectively.
    ExpectedMovedRedirect = <<"-MOVED ", (integer_to_binary(Slot))/binary, " ",
                              Addr/binary, ":",
                              (integer_to_binary(SlotPort))/binary, ?NL>>,

    TcpOpts = [binary, {active, false}, {packet, 0}],
    {ok, SlotSock}      = gen_tcp:connect("localhost", SlotPort,      TcpOpts),
    {ok, DifferentSock} = gen_tcp:connect("localhost", DifferentPort, TcpOpts),

    %% SET
    SetReq = fakeredis_encoder:encode([<<"SET">>, <<"foo">>, <<"bar">>]),
    ok = gen_tcp:send(DifferentSock, SetReq),
    {ok, WrongNodeSetResp} = gen_tcp:recv(DifferentSock, 0),

    ok = gen_tcp:send(SlotSock, SetReq),
    {ok, CorrectNodeSetResp} = gen_tcp:recv(SlotSock, 0),

    ?assertEqual(ExpectedMovedRedirect, WrongNodeSetResp),
    ?assertEqual(<<"+OK\r\n">>, CorrectNodeSetResp),

    %% GET
    GetReq = fakeredis_encoder:encode([<<"GET">>, <<"foo">>]),
    ok = gen_tcp:send(DifferentSock, GetReq),
    {ok, WrongNodeGetResp} = gen_tcp:recv(DifferentSock, 0),

    ok = gen_tcp:send(SlotSock, GetReq),
    {ok, CorrectNodeGetResp} = gen_tcp:recv(SlotSock, 0),

    ?assertEqual(ExpectedMovedRedirect, WrongNodeGetResp),
    ?assertEqual(<<"$3\r\nbar\r\n">>, CorrectNodeGetResp),

    ok = gen_tcp:close(SlotSock),
    ok = gen_tcp:close(DifferentSock).

t_ask_redirect(Config) when is_list(Config) ->
    ClusterPorts = [20040, 20041, 20042],
    fakeredis_cluster:start_link(ClusterPorts),

    Key  = <<"foo">>,
    Slot = 12182 = fakeredis_hash:hash(Key),
    {Addr, SlotPort} = fakeredis_cluster:get_node_by_slot(Slot),
    [DifferentPort | _] = [P || P <- ClusterPorts,
                                P =/= SlotPort],

    TcpOpts = [binary, {active, false}, {packet, 0}],
    {ok, SlotSock}      = gen_tcp:connect("localhost", SlotPort,      TcpOpts),
    {ok, DifferentSock} = gen_tcp:connect("localhost", DifferentPort, TcpOpts),

    %% Set some data before the ASK redirect is created
    SetReq = fakeredis_encoder:encode([<<"SET">>, <<"foo">>, <<"bar">>]),
    ok = gen_tcp:send(SlotSock, SetReq),
    ?assertEqual({ok, <<"+OK\r\n">>}, gen_tcp:recv(SlotSock, 0)),

    %% Create the ASK redirect
    fakeredis_cluster:set_ask_redirect(Key, Addr, DifferentPort),

    %% Check fakeredis_cluster:get_redirect_by_key/1.
    ?assertEqual({ask, Slot, Addr, DifferentPort},
                 fakeredis_cluster:get_redirect_by_key(<<"foo">>)),

    %% Check that a MOVED redirect is returned by the new host node if
    %% the client isn't ASKING properly.
    GetReq = fakeredis_encoder:encode([<<"GET">>, <<"foo">>]),
    ok = gen_tcp:send(DifferentSock, GetReq),
    ?assertEqual({ok, <<"-MOVED ", (integer_to_binary(Slot))/binary, " ",
                        Addr/binary, ":",
                        (integer_to_binary(SlotPort))/binary, ?NL>>},
                 gen_tcp:recv(DifferentSock, 0)),

    %% Check that the slot mapped node returns an ASK redirect
    ok = gen_tcp:send(SlotSock, GetReq),
    ?assertEqual({ok, <<"-ASK ", (integer_to_binary(Slot))/binary, " ",
                        Addr/binary, ":",
                        (integer_to_binary(DifferentPort))/binary, ?NL>>},
                 gen_tcp:recv(SlotSock, 0)),

    %% Check that the new node returns the value when ASKING properly.
    AskingReq = fakeredis_encoder:encode([<<"ASKING">>]),
    ok = gen_tcp:send(DifferentSock, [AskingReq, GetReq]),
    ?assertEqual({ok, <<"+OK\r\n">>},
                 gen_tcp:recv(DifferentSock, 0)),
    ?assertEqual({ok, <<"$3\r\nbar\r\n">>},
                 gen_tcp:recv(DifferentSock, 0)),

    %% Delete the redirect and check that fakeredis_cluster directs
    %% the key back to the original node for the slot.
    fakeredis_cluster:delete_ask_redirect(Key),
    ?assertEqual({moved, Slot, Addr, SlotPort},
                 fakeredis_cluster:get_redirect_by_key(Key)),

    ok = gen_tcp:close(SlotSock),
    ok = gen_tcp:close(DifferentSock).

t_script_handling(Config) when is_list(Config) ->
    ClusterPorts = [30000],
    fakeredis_cluster:start_link(ClusterPorts),

    {ok, Sock} = gen_tcp:connect("localhost", 30000,
                                 [binary, {active, false}, {packet, 0}]),

    %% Verify that `evalsha` is handled
    EvalShaReq = fakeredis_encoder:encode([<<"EVALSHA">>,
                                           <<"49149638cc11c5cd05b9cfdb92c8d5293becfdda">>,
                                           <<"1">>, <<"key">>, <<"val">>]),
    ok = gen_tcp:send(Sock, EvalShaReq),
    {ok, EvalShaResp} = gen_tcp:recv(Sock, 0),
    ?assertMatch(<<"-NOSCRIPT ", _/binary>>, EvalShaResp),

    %% Verify that `script` is not yet handled
    ScriptLoadReq = fakeredis_encoder:encode([<<"SCRIPT">>, <<"LOAD">>]),
    ok = gen_tcp:send(Sock, ScriptLoadReq),
    {ok, ScriptLoadResp} = gen_tcp:recv(Sock, 0),
    ?assertMatch(<<"-ERR unknown", _/binary>>, ScriptLoadResp),

    ok = gen_tcp:close(Sock).

t_kill_node_on_command(Config) when is_list(Config) ->
    Port = 30000,
    fakeredis_cluster:start_link([Port]),

    {ok, Sock} = gen_tcp:connect("localhost", Port,
                                 [binary, {active, false}, {packet, 0}]),

    %% Kill node after a <<"GET">> command was received
    fakeredis_cluster:kill_node_on_command(Port, <<"GET">>),

    SetReq = fakeredis_encoder:encode([<<"SET">>, <<"foo">>, <<"bar">>]),
    ok = gen_tcp:send(Sock, SetReq),
    {ok, _} = gen_tcp:recv(Sock, 0),

    GetReq = fakeredis_encoder:encode([<<"GET">>, <<"foo">>]),
    ok = gen_tcp:send(Sock, GetReq),
    {error, closed} = gen_tcp:recv(Sock, 0).
