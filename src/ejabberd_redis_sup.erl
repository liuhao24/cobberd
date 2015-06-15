-module(ejabberd_redis_sup).
-author('liuhao@worktile.com').

%% API
-export([start/0,
         start_link/0,
	 init/1,
	 get_pids/0,
         transform_options/1,
	 get_random_pid/0,
	 get_random_pid/1
	]).

-include("ejabberd.hrl").
-include("logger.hrl").

-define(DEFAULT_POOL_SIZE, 10).
-define(DEFAULT_REDIS_START_INTERVAL, 30). % 30 seconds
-define(DEFAULT_REDIS_HOST, "127.0.0.1").
-define(DEFAULT_REDIS_PORT, 6379).

% time to wait for the supervisor to start its child before returning
% a timeout error to the request
-define(CONNECT_TIMEOUT, 500). % milliseconds

start() ->
    case lists:any(
	   fun(Host) ->
		   is_redis_configured(Host)
	   end, ?MYHOSTS) of
	true ->
            do_start();
	false ->
	    ok
    end.

is_redis_configured(Host) ->
    ServerConfigured = ejabberd_config:get_option(
			 {redis_server, Host},
			 fun(_) -> true end, false),
    PortConfigured = ejabberd_config:get_option(
		       {redis_port, Host},
		       fun(_) -> true end, false),
    AuthConfigured = lists:member(
		       ejabberd_auth_redis,
		       ejabberd_auth:auth_modules(Host)),
    ServerConfigured or PortConfigured or AuthConfigured.

do_start() ->
    SupervisorName = ?MODULE,
    ChildSpec =
	{SupervisorName,
	 {?MODULE, start_link, []},
	 transient,
	 infinity,
	 supervisor,
	 [?MODULE]},
    case supervisor:start_child(ejabberd_sup, ChildSpec) of
	{ok, _PID} ->
	    ok;
	_Error ->
	    ?ERROR_MSG("Start of supervisor ~p failed:~n~p~nRetrying...~n",
                       [SupervisorName, _Error]),
            timer:sleep(5000),
	    start()
    end.

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    PoolSize = get_pool_size(),
    StartInterval = get_start_interval(),
    Server = get_redis_server(),
    Port = get_redis_port(),
    RedisPwd = ejabberd_config:get_option(auth_redis_pwd,
					  fun(B) when is_binary(B) -> binary_to_list(B) end,
					  undefined),
    {ok, {{one_for_one, PoolSize*10, 1},
	  lists:map(
	    fun(I) ->
		    {ejabberd_redis:get_proc(I),
		     {ejabberd_redis, start_link,
                      [I, Server, Port, RedisPwd, StartInterval*1000]},
		     transient, 2000, worker, [?MODULE]}
	    end, lists:seq(1, PoolSize))}}.

get_start_interval() ->
    ejabberd_config:get_option(
      redis_start_interval,
      fun(N) when is_integer(N), N >= 1 -> N end,
      ?DEFAULT_REDIS_START_INTERVAL).

get_pool_size() ->
    ejabberd_config:get_option(
      redis_pool_size,
      fun(N) when is_integer(N), N >= 1 -> N end,
      ?DEFAULT_POOL_SIZE).

get_redis_server() ->
    ejabberd_config:get_option(
      redis_server,
      fun(S) ->
	      binary_to_list(iolist_to_binary(S))
      end, ?DEFAULT_REDIS_HOST).

get_redis_port() ->
    ejabberd_config:get_option(
      redis_port,
      fun(P) when is_integer(P), P > 0, P < 65536 -> P end,
      ?DEFAULT_REDIS_PORT).

get_pids() ->
    [ejabberd_redis:get_proc(I) || I <- lists:seq(1, get_pool_size())].

get_random_pid() ->
    get_random_pid(now()).

get_random_pid(Term) ->
    I = erlang:phash2(Term, get_pool_size()) + 1,
    ejabberd_redis:get_proc(I).

transform_options(Opts) ->
    lists:foldl(fun transform_options/2, [], Opts).

transform_options({redis_server, {S, P}}, Opts) ->
    [{redis_server, S}, {redis_port, P}|Opts];
transform_options(Opt, Opts) ->
    [Opt|Opts].

