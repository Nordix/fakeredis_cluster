%% ets table to use
-define(STORAGE, fakeredis_cluster_storage).

-define(HASH_SLOTS, 16384).

-include_lib("hut/include/hut.hrl").

-ifdef(TEST).
%% Seems to give too long lines:
%% -define(LOG(X),    io:format(user, "[~p:L~p] " ++ X ++ "~n", [?MODULE, ?LINE])).

-define(DBG(X),    io:format(user, "\033[33mDBG: " ++ X ++ "\033[0m~n", [])).
-define(DBG(X, V), io:format(user, "\033[33mDBG: " ++ X ++ "\033[0m~n", V)).
-define(LOG(X),    io:format(user, "LOG: " ++ X ++ "\033[0m~n", [])).
-define(LOG(X, V), io:format(user, "LOG: " ++ X ++ "\033[0m~n", V)).
-define(ERR(X),    io:format(user, "\033[31mERR: " ++ X ++ "\033[0m~n", [])).
-define(ERR(X, V), io:format(user, "\033[31mERR: " ++ X ++ "\033[0m~n", V)).

-else.

-define(DBG(X),    ?log(debug, "[~p:L~p] " ++ X ++ "~n", [?MODULE, ?LINE])).
-define(DBG(X, V), ?log(debug, "[~p:L~p] " ++ X ++ "~n", [?MODULE, ?LINE]++V)).
-define(LOG(X),    ?log(notice, "[~p:L~p] " ++ X ++ "~n", [?MODULE, ?LINE])).
-define(LOG(X, V), ?log(notice, "[~p:L~p] " ++ X ++ "~n", [?MODULE, ?LINE]++V)).
-define(ERR(X),    ?log(error, X ++ "~n", [])).
-define(ERR(X, V), ?log(error, X ++ "~n", V)).
-endif.

-record(node, { id      :: binary()
              , address :: binary()
              , port    :: integer()
              }).

-record(slots_map, { start_slot = 0 :: integer()
                   , end_slot   = 0 :: integer()
                   , master_id      :: binary()
                   , replica_ids    :: [binary()] | undefined
                   }).
