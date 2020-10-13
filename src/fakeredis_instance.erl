-module(fakeredis_instance).
-behaviour(gen_server).

-include("fakeredis_common.hrl").

-export([start_link/2]).

%% gen_server callbacks
-export([init/1, handle_cast/2, handle_info/2, handle_call/3, terminate/2, code_change/3]).

-record(state, { socket,
                 transport,
                 local_address,
                 local_port,
                 remote_address,
                 remote_port,

                 %% Delay per command in milliseconds if a map is
                 %% given. The commands must be given in uppercase.
                 %% If only a number is given, it affects GET and SET.
                 delay = 0             :: timeout() |
                                          #{binary() => timeout()},
                 parser_state,

                 %% true if the previous command was ASKING
                 client_asking = false :: boolean(),

                 %% A command word, that if matching the first command in a
                 %% incoming requests, will trigger a simulated Redis node kill.
                 kill_node_on_command :: undefined | binary()
               }).

start_link(ListenSocket, Options) ->
    proc_lib:start_link(?MODULE, init, [[ListenSocket, Options]]).

init([ListenSocket, Options]) ->
    {ok, {LocalAddress, LocalPort}} = inet:sockname(ListenSocket),
    gproc:reg({p, l, {local, LocalPort}}),
    ok = proc_lib:init_ack({ok, self()}), %% Avoid block in accept/1
    %%?DBG("Waiting for client on ~s:~p...", [inet:ntoa(LocalAddress), LocalPort]),
    {ok, Socket} = gen_tcp:accept(ListenSocket),
    {ok, {RemoteAddress, RemotePort}} = inet:peername(Socket),
    gproc:reg({n, l, {remote, RemoteAddress, RemotePort}}),

    %% TLS handshake if option given
    {Transport, ActiveSocket} = case proplists:get_value(tls, Options, []) of
                                    [] -> {tcp, Socket};
                                    TlsOptions ->
                                        inet:setopts(Socket, [{active, false}]),
                                        case ssl:handshake(Socket, TlsOptions) of
                                            {ok, TlsSocket} -> {tls, TlsSocket};
                                            {error, _} -> {tcp, Socket}
                                        end
                                end,
    setopts(ActiveSocket, Transport, [{active, once}]),
    ?DBG("Client ~s:~p connected to ~s:~p.",
         [inet:ntoa(RemoteAddress), RemotePort, inet:ntoa(LocalAddress), LocalPort]),
    fakeredis_cluster:log_event(connect, #{local_address  => LocalAddress,
                                           local_port     => LocalPort,
                                           remote_address => RemoteAddress,
                                           remote_port    => RemotePort,
                                           transport      => Transport}),

    Delay = proplists:get_value(delay, Options, 0),
    gen_server:enter_loop(?MODULE, [], #state{socket = ActiveSocket,
                                              transport = Transport,
                                              local_address = LocalAddress,
                                              local_port = LocalPort,
                                              remote_address = RemoteAddress,
                                              remote_port = RemotePort,
                                              delay = Delay,
                                              parser_state = fakeredis_parser:init()
                                             }).

handle_cast({set_kill_node_on_command, Command}, State) ->
    {noreply, State#state{kill_node_on_command = Command}};
handle_cast(_, State) ->
    {noreply, State}.

handle_call(exit, _From, State) ->
    {stop, "Exit called", State};
handle_call(_E, _From, State) -> {noreply, State}.

%% TCP/TLS events
handle_info({Tag, Socket, Data}, #state{socket = Socket,
                                        transport = Transport} = State)
  when Tag =:= tcp; Tag =:= ssl ->
    ok = setopts(Socket, Transport, [{active, once}]),
    {noreply, parse_data(Data, State)};
handle_info({Closed, _Socket}, State)
  when Closed =:= tcp_closed; Closed =:= ssl_closed ->
    fakeredis_cluster:log_event(disconnect, normal),
    {stop, normal, State#state{socket = undefined}};
handle_info({Error, _Socket, Reason}, State)
  when Error =:= tcp_error; Error =:= ssl_error ->
    fakeredis_cluster:log_event(disconnect, {error, Reason}),
    {stop, normal, State};
handle_info(E, State) ->
    ?ERR("unexpected: ~p (State: ~p)", [E, State]),
    {noreply, State}.

terminate(Reason, State) ->
    case State of
        #state{socket = undefined} -> ok;       % already closed
        #state{transport = tcp, socket = Socket} -> gen_tcp:close(Socket);
        #state{transport = tls, socket = Socket} -> ssl:close(Socket)
    end,
    ?DBG("fakeredis_intance:terminate(Reason=~p, Port=~p, ClientPort=~p)",
         [Reason, State#state.local_port, State#state.remote_port]),
    ok.

code_change(_OldVersion, Tab, _Extra) -> {ok, Tab}.

-spec parse_data(Data :: binary(), State :: #state{}) -> NewState :: #state{}.
parse_data(Data, #state{parser_state = ParserState} = State) ->
    case fakeredis_parser:parse(ParserState, Data) of
        %% Got complete request
        {ok, Value, NewParserState} ->
            NewState = handle_data(Value, State),
            NewState#state{parser_state = NewParserState};

        %% Got complete request, with extra data
        {ok, Value, Rest, NewParserState} ->
            NewState = handle_data(Value, State),
            parse_data(Rest, NewState#state{parser_state = NewParserState});

        %% Parser needs more data
        {continue, NewParserState} ->
            State#state{parser_state = NewParserState};

        %% Error
        {error, unknown_response} ->
            ?ERR("Unknown data: ~p", [Data]),
            State
    end.

-spec handle_data(Command :: list(), #state{}) -> #state{}.
handle_data([Cmd | _], #state{kill_node_on_command = Cmd,
                              local_port = Port}) ->
    gen_server:call(fakeredis_cluster, {kill_node, Port});
handle_data([Cmd | Args] = Data, State) ->
    %% Make sure first command is in upper
    CmdUpper = string:uppercase(Cmd),
    command_delay(CmdUpper, State),
    fakeredis_cluster:log_event(command, Data),
    handle_request([CmdUpper | Args], State),
    %% Set flag indicating if the previous command was ASKING
    State#state{client_asking = CmdUpper =:= <<"ASKING">>}.

handle_request([<<"CLUSTER">>, Type | _Rest], State) ->
    ?DBG("Requesting CLUSTER"),
    case string:uppercase(Type) of
        <<"SLOTS">> ->
            {ok, Msg} = gen_server:call(fakeredis_cluster, cluster_slots),
            encode_and_send(Msg, State);
        _ ->
            encode_and_send({error, <<"Command not handled: CLUSTER ", Type/binary>>},
                            State)
    end;

handle_request([<<"SET">>, Key, Value | _Tail], State) ->
    Response =
        case check_redirect(Key, State) of
            ok ->
                %% Not a redirect. The key is served by us.
                ets:insert(?STORAGE, {Key, Value}),
                ok;
            {error, _RedirectMsg} = Error ->
                Error
        end,
    encode_and_send(Response, State);

handle_request([<<"GET">>, Key | _Tail], State) ->
    Response =
        case check_redirect(Key, State) of
            ok ->
                %% Not a redirect. The key is served by us.
                case ets:lookup(?STORAGE, Key) of
                    [{Key, Val}] ->
                        Val;
                    [] ->
                        null_bulkstring
                end;
            {error, _RedirectMsg} = Error ->
                Error
        end,
    encode_and_send(Response, State);

handle_request([<<"ASKING">>], State) ->
    %% The client_asking flag is handled by handle_data/2.
    encode_and_send(ok, State);

handle_request([<<"EVALSHA">> | _Tail], State) ->
    %% No script handling yet in fakeredis_cluster
    %% Reply like Redis when there is no script available
    encode_and_send({error, <<"NOSCRIPT No matching script. Please use EVAL.">>},
                    State);

handle_request([Cmd | _Tail], State) ->
    encode_and_send({error, <<"ERR unknown command `", Cmd/binary,
                              "` (but this is fakeredis...)">>},
                    State).

%% Checks if a key is redirected to another node. Returns an error
%% with a redirect encoded as a redis error response or ok if the
%% request should be serverd by the current node.
-spec check_redirect(Key :: binary(), State :: #state{}) ->
          ok | {error, EncodedRedisError :: binary()}.
check_redirect(Key, #state{local_port = LocalPort, client_asking = Asking}) ->
    case fakeredis_cluster:get_redirect_by_key(Key) of
        {moved, _Slot, _Addr, LocalPort} ->
            %% The slot belongs to the current node.
            ok;
        {ask, _Slot, _Addr, LocalPort} when Asking ->
            %% There is an ASK redirect to the current node for this
            %% key and the client is asking properly.
            ok;
        {ask, Slot, _Addr, LocalPort} when not Asking ->
            %% This node has the key, but the client didn't ask
            %% properly.  Redirect back to the node which would
            %% normally serve this slot.
            {Addr, Port} = fakeredis_cluster:get_node_by_slot(Slot),
            {error, encode_redirect({moved, Slot, Addr, Port})};
        Redirect ->
            %% ASK or MOVED to another node
            {error, encode_redirect(Redirect)}
    end.

%% Encodes a redirect tuple as a binary message. The "-" prefix is not
%% added.
encode_redirect({Redirect, Slot, Addr, Port}) ->
    Tag = case Redirect of
              ask   -> <<"ASK">>;
              moved -> <<"MOVED">>
          end,
    <<Tag/binary, " ", (integer_to_binary(Slot))/binary, " ",
      Addr/binary, ":", (integer_to_binary(Port))/binary>>.

setopts(Socket, tcp, Opts) -> inet:setopts(Socket, Opts);
setopts(Socket, tls, Opts) ->  ssl:setopts(Socket, Opts).

%% Redis-encodes and sends data (a reply to a command) to the client
encode_and_send(Data, State) ->
    fakeredis_cluster:log_event(reply, Data),
    Encoded = fakeredis_encoder:encode(Data),
    case State#state.transport of
        tls ->
            ok = ssl:send(State#state.socket, Encoded),
            ssl:setopts(State#state.socket, [{active, once}]);
        tcp ->
            ok = gen_tcp:send(State#state.socket, Encoded),
            inet:setopts(State#state.socket, [{active, once}])
    end.

%% Sleeps for a number of milliseconds as configured for the given command
command_delay(Command, #state{delay = CommandDelay})
  when is_map(CommandDelay) ->
    %% Delay by command
    case CommandDelay of
        #{Command := Delay} ->
            timer:sleep(Delay);
        _NoMatch ->
            ok
    end;
command_delay(Command, #state{delay = Delay})
  when is_integer(Delay) orelse Delay =:= infinity,
       Command =:= <<"GET">> orelse Command =:= <<"SET">> ->
    %% Old numeric delay option affects GET and SET
    timer:sleep(Delay);
command_delay(_Command, _State) ->
    ok.
