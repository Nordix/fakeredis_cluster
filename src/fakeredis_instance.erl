-module(fakeredis_instance).
-behaviour(gen_server).

-include("fakeredis_common.hrl").

-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_cast/2, handle_info/2, handle_call/3, terminate/2, code_change/3]).

-record(state, { socket,
                 local_address,
                 local_port,
                 remote_address,
                 remote_port,
                 parser_state
               }).

start_link(ListenSocket) ->
    proc_lib:start_link(?MODULE, init, [ListenSocket]).

init(ListenSocket) ->
    {ok, {LocalAddress, LocalPort}} = inet:sockname(ListenSocket),
    gproc:reg({p, l, {local, LocalPort}}),
    ok = proc_lib:init_ack({ok, self()}), %% Avoid block in accept/1
    ?DBG("Listening on ~p:~p", [LocalAddress, LocalPort]),

    ?DBG("Waiting for client to accept..."),
    {ok, AcceptSocket} = gen_tcp:accept(ListenSocket),
    {ok, {RemoteAddress, RemotePort}} = inet:peername(AcceptSocket),
    gproc:reg({n, l, {remote, RemoteAddress, RemotePort}}),
    ?DBG("Client ~p:~p accepted...", [RemoteAddress, RemotePort]),
    gen_server:enter_loop(?MODULE, [], #state{socket = AcceptSocket,
                                              local_address = LocalAddress,
                                              local_port = LocalPort,
                                              remote_address = RemoteAddress,
                                              remote_port = RemotePort,
                                              parser_state = eredis_parser:init()
                                             }).

handle_cast(_, State) ->
    {noreply, State}.

handle_call(exit, _From, State) ->
    {stop, "Exit called", State};
handle_call(_E, _From, State) -> {noreply, State}.

%% Connection calls
handle_info({tcp, Socket, Data}, #state{socket = Socket} = State) ->
    ok = inet:setopts(Socket, [{active, once}]),
    {noreply, parse_data(Data, State)};
handle_info({tcp_closed, _Socket}, State) -> {stop, normal, State};
handle_info({tcp_error, _Socket, _}, State) -> {stop, normal, State};
handle_info(E, State) ->
    ?ERR("unexpected: ~p", [E]),
    {noreply, State}.

terminate(_Reason, _Tab) ->
    ?DBG("fakeredis_intance terminated"),
    ok.
code_change(_OldVersion, Tab, _Extra) -> {ok, Tab}.

parse_data(Data, #state{parser_state = ParserState} = State) ->
    case eredis_parser:parse(ParserState, Data) of
        %% Got complete request
        {ok, Value, NewParserState} ->
            handle_data(State, Value),
            State#state{parser_state = NewParserState};

        %% Got complete request, with extra data
        {ok, Value, Rest, NewParserState} ->
            handle_data(State, Value),
            parse_data(Rest, #state{parser_state = NewParserState});

        %% Parser needs more data
        {continue, NewParserState} ->
            State#state{parser_state = NewParserState};

        %% Error
        {error, unknown_response} ->
            ?ERR("Unknown data: ~p", [Data]),
            {noreply, State}
    end.

%% Make sure first command is in upper
handle_data(State, [Cmd | Data]) ->
    handle_request(State, lists:flatten([string:uppercase(Cmd), Data])).

handle_request(State, [<<"CLUSTER">> | [Type | _Rest]]) ->
    ?DBG("Requesting CLUSTER"),
    case string:uppercase(Type) of
        <<"SLOTS">> ->
            {ok, Msg} = gen_server:call(fakeredis_cluster, cluster_slots),
            send(State#state.socket, Msg);
        _ ->
            ?ERR("Not handled cmd: CLUSTER ~p~n", [Type])
    end;

handle_request(State, [<<"SET">>, Key, Value | _Tail]) ->
    ?DBG("SET request for key: ~p and value: ~p", [Key, Value]),
    ets:insert(?STORAGE, {Key, Value}),
    Msg = fakeredis_encoder:encode(ok),
    send(State#state.socket, Msg);

handle_request(State, [<<"GET">>, Key | _Tail]) ->
    ?DBG("GET request for key: ~p", [Key]),
    Value = case ets:lookup(?STORAGE, Key) of
                [{Key, Val}] ->
                    Val;
                [] ->
                    null_bulkstring
            end,
    Msg = fakeredis_encoder:encode(Value),
    send(State#state.socket, Msg).

send(Socket, Str) ->
    ?DBG("SEND: ~p", [Str]),
    ok = gen_tcp:send(Socket, Str),
    ok = inet:setopts(Socket, [{active, once}]),
    ok.




