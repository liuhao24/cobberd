-module(cobber_channel).
-author('liuhao').

-behaviour(gen_mod).

-behaviour(gen_server).

-include("ejabberd.hrl").
-include("logger.hrl").

-include("jlib.hrl").

-define(SUPERVISOR, ejabberd_sup).

-define(PRE_UID, <<"uid:">>).
-define(PRE_LOGIN_TIME, <<"dur_">>).

-define(USER_PRESENCE, <<"presence_">>).
-define(ONLINE, 1).
-define(OFFLINE, 0).
-define(BUSY, 2).
-define(LEAVE, 3).

-define(TO_CHANNEL, 1).

-define(RETAINS, retains).

%% API
-export([start_link/2, pub_to_channel/5]).

%% gen_mod callbacks
-export([start/2, stop/1]).

%% gen_server callbacks
-export([init/1, terminate/2, handle_call/3,
	 handle_cast/2, handle_info/2, code_change/3]).

%% Hook callbacks
-export([user_available/1, unset_presence/3, set_presence/4, append_to_channel/3, rm_from_channel/3]).

-record(state,
	{host = <<"">>, server_host, bot = <<>>}).



%%====================================================================
%% API
%%====================================================================
start_link(Host, Opts) ->
    Proc = gen_mod:get_module_proc(Host, ?MODULE),
    gen_server:start_link({local, Proc}, ?MODULE,
			  [Host, Opts], []).

user_available(New) ->
    LUser = New#jid.luser,
    LServer = New#jid.lserver,
    Proc = gen_mod:get_module_proc(LServer, ?MODULE),
    gen_server:cast(Proc, {user_available, LUser, LServer}).

unset_presence(_SID, JID, _Info) ->
    LUser = JID#jid.luser,
    LServer = JID#jid.lserver,
    Resource = JID#jid.lresource,
    Proc = gen_mod:get_module_proc(LServer, ?MODULE),
    gen_server:cast(Proc, {user_offline, LUser, LServer, Resource}).

set_presence(User, Server, Resource, Packet) ->
    %%{xmlelement, Tag, Attrs, _} = Packet,
    Proc = gen_mod:get_module_proc(Server, ?MODULE),
    gen_server:cast(Proc, {set_presence, User, Server, Resource, Packet}).


append_to_channel(Uid, Cid, Host) ->
    Proc = gen_mod:get_module_proc(Host, ?MODULE),
    gen_server:call(Proc, {append_to_channel, Uid, Cid}).

rm_from_channel(Uid, Cid, Host) ->
    Proc = gen_mod:get_module_proc(Host, ?MODULE),
    gen_server:call(Proc, {rm_from_channel, Uid, Cid}).

pub_to_channel(Host, Channel, Ft, Fid, Msg) ->
    Proc = gen_mod:get_module_proc(Host, ?MODULE),
    gen_server:call(Proc, {pub_to_channel, Channel, Ft, Fid, Msg}).

%%====================================================================
%% gen_mod callbacks
%%====================================================================
start(Host, Opts) ->
    Proc = gen_mod:get_module_proc(Host, ?MODULE),
    ChildSpec = {Proc, {?MODULE, start_link, [Host, Opts]},
		transient, 2000, worker, [?MODULE]},
    supervisor:start_child(?SUPERVISOR, ChildSpec).

stop(Host) ->
    Proc = gen_mod:get_module_proc(Host, ?MODULE),
    gen_server:call(Proc, stop),
    supervisor:delete_child(?SUPERVISOR, Proc).

%%====================================================================
%% gen_server callbacks
%%====================================================================
init([Host, Opts]) ->
    MyHost = gen_mod:get_opt_host(Host, Opts,
				  <<"channel.@HOST@">>),
    ejabberd_hooks:add(set_presence_hook, Host,
    			     ?MODULE, set_presence, 100),
    ejabberd_hooks:add(user_available_hook, Host, ?MODULE,
		       user_available, 50),
    ejabberd_hooks:add(sm_remove_connection_hook, Host, ?MODULE,
		       unset_presence, 50),

    ejabberd_router:register_route(MyHost),

    {ok, #state{host = Host, server_host = MyHost}}.

terminate(_Reason, #state{host = Host}) ->
    ejabberd_hooks:delete(set_presence_hook, Host,
     			  ?MODULE, set_presence, 100),
    ejabberd_hooks:delete(user_available_hook, Host,
			  ?MODULE, user_available, 50),
    ejabberd_hooks:delete(sm_remove_connection_hook, Host,
			  ?MODULE, unset_presence, 50),
    ok.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call({pub_to_channel, Channel, Ft, Fid, Msg}, _From, #state{host=Host, server_host=ServerHost} = State) ->

    {reply, ok, State};
handle_call({append_to_channel, Uid, Cid}, _From, State) ->

    {reply, ok, State};
handle_call({rm_from_channel, Uid, Cid}, _From, State) ->

    {reply, ok, State};
handle_call(_Req, _From, State) ->
    {reply, {error, badarg}, State}.

handle_cast({set_presence, User, Server, Resource, Packet}, State) ->
    set_user_presence(User, Server, Resource, Packet),
    {noreply, State};
handle_cast({user_available, LUser, LServer}, State) ->
    append2online(LUser, LServer, State),
    {noreply, State};
handle_cast({user_offline, LUser, LServer, Resource}, State) ->
    remove4online(LUser, LServer, Resource),
    {noreply, State};
handle_cast(_Msg, State) -> {noreply, State}.

handle_info({route, From, To, Packet},
	    #state{host = Host, server_host = MyHost} = State) ->
    case catch do_route(Host, MyHost, From, To, Packet) of
	{'EXIT', Reason} ->
	    ?ERROR_MSG("~p", [Reason]);
	_ ->
	    ok
    end,
    {noreply, State};
handle_info(_Info, State) -> {noreply, State}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%====================================================================
%% internal methods
%%====================================================================
append2online(Uid, Server, _State) ->
    TeamId = ejabberd_mongo:get_team(Uid),
    ejabberd_sm_redis:append2online(TeamId, Uid),
    ok.

set_user_presence(User, Server, _Resource, Packet) ->
    Presence = case xml:get_subtag_cdata(Packet, <<"show">>) of 
        <<"away">> ->   ?LEAVE;
	<<"offline">> ->  ?OFFLINE;
	<<"busy">> ->   ?BUSY;
	_ ->     ?ONLINE
    end,
    ejabberd_sm_redis:set_presence(User, Presence),
    case ejabberd_sm_redis:get_team_online(User) of
	 [] ->
	     ok;
	 Members ->
	     Fjid = jlib:make_jid(User, Server, <<"">>),
	     #xmlel{name=Name, attrs=Attrs, children=Childs} = Packet,
	     Attrs1 = Attrs ++ [{<<"from">>, jlib:jid_to_string(Fjid)}],
	     Packet1 = #xmlel{name=Name, attrs=Attrs1, children=Childs},
	     SendFun = fun(Uid) when Uid /= User -> 
			       ToJid = jlib:make_jid(Uid, Server, <<"">>),
			       ejabberd_sm:route(Fjid, ToJid, Packet1);
			   (_) -> ok
		       end,
	     [SendFun(M) || M <- Members]
    end,
    ok.

remove4online(LUser, LServer, _Resource) ->
    Resources = ejabberd_sm:get_user_resources(LUser, LServer),
    case length(Resources) of
	0 ->
	    ejabberd_sm_redis:set_presence(LUser, ?OFFLINE), 
	    case ejabberd_sm_redis:get_team_online(LUser) of
		[] -> ok;
		Members ->
		    Fjid = jlib:make_jid(LUser, LServer, <<"">>),
		    Packet = offline_packet(Fjid),
		    SendFun = fun(Uid) when Uid /= LUser -> 
				      ToJid = jlib:make_jid(Uid, LServer, <<"">>),
				      ejabberd_sm:route(Fjid, ToJid, Packet);
				 (_) -> ok
			      end,
		    [SendFun(M) || M <- Members]
	    end;
	_ -> ok
    end,
    ok.

do_route(Host, ServerHost, From, To, Packet) ->
    {Room, _, _Nick} = jlib:jid_tolower(To),
    #xmlel{name = Name, attrs = Attrs} = Packet,
    case {Room, Name} of
      {<<"">>, _} ->
	    ok;
      {_, <<"message">>} ->
            Type = xml:get_attr_s(<<"type">>, Attrs),
	    ToType = xml:get_subtag_cdata(Packet, <<"tt">>),
	    do_route2({Type, ToType}, [Room, From, To, Packet, Host, ServerHost]);
      {_, _} ->
	    ok
    end.

do_route2({<<"groupchat">>, <<"1">>}, Args) ->
    [Channel, From, To, Packet, Host, ServerHost] = Args,
    #jid{luser = FromUid} = From,
    Members =  ejabberd_mongo:get_channel_member(Channel),
    FromJid = jlib:make_jid(Channel, ServerHost, FromUid),
    lists:foreach(%% send to channel online users
      fun(Muid) when Muid /= <<"">> ->
	      ejabberd_router:route(
		FromJid,
		jlib:make_jid(Muid, Host, <<"">>),
		Packet);
	 (_) -> ok
      end,
      Members);
do_route2(_, _) ->
    ok.

offline_packet(From) ->
    Attrs = [{<<"xml:lang">>, <<"en">>},
	     {<<"type">>, <<"unavailable">>},
	     {<<"from">>, jlib:jid_to_string(From)}
	    ],
    Blank = {xmlcdata, <<"\n">>},
    {xmlel, <<"presence">>, Attrs, [Blank]}.


