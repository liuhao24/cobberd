-module(ejabberd_redis).
-behaviour(gen_server).

%% API
-export([start_link/1, start_link/5, get_proc/1, set/2, get/1, sunion/1, sadd/2, srem/2,smembers/1]).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("ejabberd.hrl").
-include("logger.hrl").

-record(state, {pid = self() :: pid()}).


%%%===================================================================
%%% API
%%%===================================================================
%% @private
start_link(Num, Server, Port, Pwd,  _StartInterval) ->
    gen_server:start_link({local, get_proc(Num)}, ?MODULE, [Server, Port, Pwd], []).

start_link({Server, Port, Pwd}) ->
    gen_server:start_link(?MODULE, [Server, Port, Pwd], []).

%% @private
get_proc(I) ->
    jlib:binary_to_atom(
      iolist_to_binary(
	[atom_to_list(?MODULE), $_, integer_to_list(I)])).

%%%----------------------------------------------------------------------
%%% API
%%%----------------------------------------------------------------------
get(Key) ->
    Pid = get_random_pid(),
    case eredis:q(Pid, ["GET", Key]) of
	{ok, Obj} when Obj /= undefined ->
	    Obj;
	_Any ->
	    none
    end.

set(Key, Value) ->
    Pid = get_random_pid(),
    eredis:q(Pid, ["SET", Key, Value]).

sunion(Keys) ->
    Pid = get_random_pid(),
    case eredis:q(Pid, ["SUNION"] ++ Keys) of
	{ok, Obj} when Obj /= undefined ->
	    Obj;
	_Any ->
	    none
    end.

sadd(Key, Value) ->
    Pid = get_random_pid(),
    eredis:q(Pid, ["SADD", Key, Value]).

srem(Key, Value) ->
    Pid = get_random_pid(),
    eredis:q(Pid, ["SREM", Key, Value]).

smembers(Key) ->
    Pid = get_random_pid(),
    case eredis:q(Pid, ["SMEMBERS", Key]) of
	{ok, T} when is_list(T)->
	    T;
	_ ->
	    none
    end.

%%%----------------------------------------------------------------------
%%% Internal API
%%%----------------------------------------------------------------------
get_random_pid() ->
    PoolPid = ejabberd_redis_sup:get_random_pid(),
    case catch gen_server:call(PoolPid, get_pid) of
	{ok, Pid} ->
	    Pid;
	{'EXIT', {timeout, _}} ->
	    throw({error, timeout});
	{'EXIT', Err} ->
	    throw({error, Err})
    end.


%%%===================================================================
%%% gen_server API
%%%===================================================================
%% @private
init([Server, Port, undefined]) ->
    case eredis:start_link(Server, Port) of
        {ok, Pid} ->
            %% erlang:monitor(process, Pid),
            {ok, #state{pid = Pid}};
        Err ->
            {stop, Err}
    end;
init([Server, Port, Pwd]) ->
    case eredis:start_link(Server, Port, 0, Pwd) of
        {ok, Pid} ->
            %% erlang:monitor(process, Pid),
            {ok, #state{pid = Pid}};
        Err ->
            {stop, Err}
    end.

%% @private
handle_call(get_pid, _From, #state{pid = Pid} = State) ->
    {reply, {ok, Pid}, State};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%% @private
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
handle_info({'DOWN', _MonitorRef, _Type, _Object, _Info}, State) ->
    {stop, normal, State};
handle_info(_Info, State) ->
    ?ERROR_MSG("unexpected info: ~p", [_Info]),
    {noreply, State}.

%% @private
terminate(_Reason, _State) ->
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

