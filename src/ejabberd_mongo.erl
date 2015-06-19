-module(ejabberd_mongo).

-behaviour(ejabberd_config).

-author('liuhao').

%% API
-export([start/0,
         start_link/0,
	 init/1,
	 opt_type/1,
	 objectid_to_binary/1,
	 binary_to_objectid/1,         
	 get_passwd/1,
	 get_team/1,
	 get_team_member/1,
	 get_channel_member/1,
	 get_team_by_channel/1
	]).

-include("ejabberd.hrl").
-include("logger.hrl").

-define(DEFAULT_MAX_OVERFLOW, 15).
-define(DEFAULT_POOL_SIZE, 10).
-define(DEFAULT_START_INTERVAL, 30). % 30 seconds
-define(DEFAULT_MONGO_HOST, "127.0.0.1").
-define(DEFAULT_MONGO_PORT, 27017).

% time to wait for the supervisor to start its child before returning
% a timeout error to the request
-define(CONNECT_TIMEOUT, 500). % milliseconds
-define(MONGOPOOL, mongopool).
-define(MAX, 50000).
-define(COLL_USER, user).
-define(COLL_TEAM, team).
-define(COLL_CHANNEL, channel).

start() ->
    case lists:any(
	   fun(Host) ->
		   is_mongo_configured(Host)
	   end, ?MYHOSTS) of
	true ->
            do_start();
	false ->
	    ok
    end.

is_mongo_configured(Host) ->
    ServerConfigured = ejabberd_config:get_option(
			 {mongo_server, Host},
			 fun(_) -> true end, false),
    PortConfigured = ejabberd_config:get_option(
		       {mongo_port, Host},
		       fun(_) -> true end, false),
    ServerConfigured or PortConfigured.

do_start() ->
    SupervisorName = ?MODULE,	       
    %% ChildSpec =
    %% 	{SupervisorName,
    %% 	 {?MODULE, start_link, []},
    %% 	 transient,
    %% 	 infinity,
    %% 	 supervisor,
    %% 	 [?MODULE]},
    PoolSize = get_pool_size(),
    Server = get_mongo_server(),
    Port = get_mongo_port(),
    Db = get_mongo_db(),
    Maxoverflow = get_max_overflow(),
    ChildSpec = mongo_pool:child_spec(?MONGOPOOL, PoolSize, Server, Port, Db, Maxoverflow),

    case supervisor:start_child(ejabberd_sup, ChildSpec) of
	{ok, PID} ->
	    ?INFO_MSG("start mongo pool: ~p ~n", [PID]),
	    ok;
	_Error ->
	    ?ERROR_MSG("Start of supervisor ~p failed:~n~p~nRetrying...~n",
                       [SupervisorName, _Error]),
            timer:sleep(5000),
	    start()
    end.

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

get_passwd(Uid) ->
    Doc = mongo_pool:find_one(?MONGOPOOL, ?COLL_USER, {'_id', binary_to_objectid(Uid)}, {'pass', 1}),
    case Doc of
	{Doc1} ->
	    case bson:lookup(pass, Doc1) of
		{M}  ->
		    M;
		_ ->
		    <<"">>
	    end;
	_ ->
	    <<"">>
    end.

get_team(Uid) ->
    Doc = mongo_pool:find_one(?MONGOPOOL, ?COLL_USER, {'_id', binary_to_objectid(Uid)}, {'team', 1}),
    case Doc of
	{Doc1} ->
	    case bson:lookup(team, Doc1) of
		{M}  ->
		    M;
		_ ->
		    <<"">>
	    end;
	_ ->
	    <<"">>
    end.

get_team_by_channel(Cid) ->
    Doc = mongo_pool:find_one(?MONGOPOOL, ?COLL_CHANNEL, {'_id', binary_to_objectid(Cid)}, {'team', 1, 'type', 1}),
    case Doc of
	{Doc1} ->
	    TeamDoc = bson:lookup(team, Doc1),
	    TypeDoc = bson:lookup(type, Doc1),
	    case {TeamDoc, TypeDoc} of
		{{Team},{TeamType}}  ->
		    [Team, TeamType];
		_ -> []		    
	    end;
	_ -> []
    end.

get_team_member(Team) ->
    Docs = mongo_pool:find(?MONGOPOOL, ?COLL_USER, {'team', Team}, {'_id', 1}),
    case Docs of
	false -> [];
	none -> [];
	Cursor ->
	    Rs = take(Cursor, ?MAX, desc),
	    [objectid_to_binary(X) || {'_id', X} <- Rs]
    end.

get_channel_member(Cid) ->
    case get_team_by_channel(Cid) of
	[Team, 0] -> 
	    get_team_member(Team);
	[Team, _Type] ->
	    Doc = mongo_pool:find_one(?MONGOPOOL, ?COLL_CHANNEL, {'_id', binary_to_objectid(Cid)}, 
				  {'members.uid', 1}),
	    case Doc of
		{} -> [];
		{Doc1} ->
		    case bson:lookup(members, Doc1) of
			{} -> [];
			{M} when is_list(M) ->
			    [X || {uid, X} <- M];
			_ ->
			    []
		    end;
		_ ->[]
	    end;
	_ -> []
    end.


init([]) ->
    PoolSize = get_pool_size(),
    Server = get_mongo_server(),
    Port = get_mongo_port(),
    Db = get_mongo_db(),
    %% User = ejabberd_config:get_option(
    %% 	     mongo_user,
    %% 	     fun(S) ->
    %% 		     iolist_to_binary(S)
    %% 	     end, <<"">>),
    %% Pwd = ejabberd_config:get_option(
    %% 	     mongo_pwd,
    %% 	     fun(S) ->
    %% 		     iolist_to_binary(S)
    %% 	     end, <<"">>),
    Maxoverflow = get_max_overflow(),
    ChildSpec = mongo_pool:child_spec(?MONGOPOOL, PoolSize, Server, Port, Db, Maxoverflow),
    case supervisor:start_child(ejabberd_sup, ChildSpec) of
	{ok, _PID} ->
	    ok;
	_Error ->
	    ?ERROR_MSG("Start of supervisor ~p failed:~n~p~nRetrying...~n",
                       [mongopool, _Error]),
            timer:sleep(5000),
	    start()
    end.

get_max_overflow() ->
    ejabberd_config:get_option(
      mongo_max_overflow,
      fun(N) when is_integer(N), N >= 1 -> N end,
      ?DEFAULT_MAX_OVERFLOW).

get_pool_size() ->
    ejabberd_config:get_option(
      mongo_pool_size,
      fun(N) when is_integer(N), N >= 1 -> N end,
      ?DEFAULT_POOL_SIZE).

get_mongo_server() ->
    ejabberd_config:get_option(
      mongo_server,
      fun(S) ->
	      binary_to_list(iolist_to_binary(S))
      end, ?DEFAULT_MONGO_HOST).

get_mongo_port() ->
    ejabberd_config:get_option(
      mongo_port,
      fun(P) when is_integer(P), P > 0, P < 65536 -> P end,
      ?DEFAULT_MONGO_PORT).

get_mongo_db() ->
    ejabberd_config:get_option(
      mongo_db,
      fun(DB) when is_atom(DB) -> DB end,
      test).

take(Cursor, Count, Order) ->
    Result = take_inner(Cursor, Count, []),
    mc_cursor:close(Cursor),
    case Order of
        desc -> Result;
        asc  -> lists:reverse(Result)
    end.

take_inner(Cursor, Count, Acc) when Count > 0 ->
    case mc_cursor:next(Cursor) of
        {}  -> Acc;
        {X} -> take_inner(Cursor, Count-1, [X|Acc])
    end;
take_inner(_Cursor, _Count, Acc) -> Acc.

iolist_to_list(IOList) ->
    binary_to_list(iolist_to_binary(IOList)).

objectid_to_binary({Id}) -> objectid_to_binary(Id, []).

objectid_to_binary(<<>>, Result) ->
    jlib:tolower(list_to_binary(lists:reverse(Result)));
objectid_to_binary(<<Hex:8, Bin/binary>>, Result) ->
    SL1 = erlang:integer_to_list(Hex, 16),
    SL2 = case erlang:length(SL1) of
        1 -> ["0"|SL1];
        _ -> SL1
    end,
    objectid_to_binary(Bin, [SL2|Result]).

binary_to_objectid(BS) -> binary_to_objectid(BS, []).

binary_to_objectid(<<>>, Result) ->
    {list_to_binary(lists:reverse(Result))};
binary_to_objectid(<<BS:2/binary, Bin/binary>>, Result) ->
    binary_to_objectid(Bin, [erlang:binary_to_integer(BS, 16)|Result]).

us_to_key({LUser, LServer}) ->
    <<"ejabberd:sm:", LUser/binary, "@", LServer/binary>>.

opt_type(mongo_max_overflow) ->
    fun (I) when is_integer(I), I > 0 -> I end;
opt_type(mongo_db) ->
    fun (I) when is_atom(I) -> I end;
opt_type(mongo_password) -> fun iolist_to_list/1;
opt_type(mongo_port) ->
    fun (P) when is_integer(P), P > 0, P < 65536 -> P end;
opt_type(mongo_pool_size) ->
    fun (I) when is_integer(I), I > 0 -> I end;
opt_type(mongo_server) -> fun iolist_to_list/1;
opt_type(_) ->
    [mongo_pool_size, mongo_db, mongo_pwd, mongo_max_overflow,
     mongo_port, mongo_server].
