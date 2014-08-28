-module(ejournald_io_server).

-export([start_link/0, init/0, loop/1, get_line/2, get_chars/3]).

-define(CHARS_PER_REC, 10).

-record(state, {
	  fd, 			% file descriptor
	  fd_stream, 	% file descriptor
	  position, 	% absolute
	  mode 			% binary | list
	 }).

%% -------------------------------------------------------------------------
%% -- IO - Protocol Interface
start_link() ->
    spawn_link(?MODULE,init,[]).

init() ->
	{ok, Fd} = journald_api:open(),
	ok = journald_api:seek_head(Fd),
	ok = journald_api:next(Fd),
    Fd_stream = journald_api:stream_fd("ejournald_io_server", 5, 0),
    ?MODULE:loop(#state{fd = Fd, fd_stream = Fd_stream, position = 0, mode=list}).

loop(State) ->
    receive
	{io_request, From, ReplyAs, Request} ->
	    case request(Request,State) of
			{Tag, Reply, NewState} when Tag =:= ok; Tag =:= error ->
			    reply(From, ReplyAs, Reply),
			    ?MODULE:loop(NewState);
			{stop, Reply, _NewState} ->
			    reply(From, ReplyAs, Reply),
			    exit(Reply)
	    end;
	%% Private message
	{From, rewind} ->
	    From ! {self(), ok},
	    ?MODULE:loop(State#state{position = 0});
	_Unknown ->
	    ?MODULE:loop(State)
    end.

reply(From, ReplyAs, Reply) ->
 	From ! {io_reply, ReplyAs, Reply}.

%% ---------------------------------------------------------------------------
%% -- writing data
request({put_chars, Encoding, Chars}, State) ->
    put_chars(unicode:characters_to_list(Chars,Encoding),State);
request({put_chars, Encoding, Module, Function, Args}, State) ->
    try
		request({put_chars, Encoding, apply(Module, Function, Args)}, State)
    catch
		_:_ ->
	    	{error, {error,Function}, State}
    end;

%% ---------------------------------------------------------------------------
%% -- retrieving data
request({get_until, Encoding, _Prompt, M, F, As}, State) ->
    get_until(Encoding, M, F, As, State);
request({get_chars, Encoding, _Prompt, N}, State) ->
    get_until(Encoding, ?MODULE, get_chars, [N], State);
request({get_line, Encoding, _Prompt}, State) ->
    get_until(Encoding, ?MODULE, get_line, [], State);

%% ---------------------------------------------------------------------------
%% -- other helpers
request({get_geometry,_}, State) ->
    {error, {error,enotsup}, State};
request({setopts, Opts}, State) ->
    setopts(Opts, State);
request(getopts, State) ->
    getopts(State);
request({requests, Reqs}, State) ->
     multi_request(Reqs, {ok, ok, State});
request({put_chars,Chars}, State) ->
    request({put_chars,latin1,Chars}, State);
request({put_chars,M,F,As}, State) ->
    request({put_chars,latin1,M,F,As}, State);
request({get_chars,Prompt,N}, State) ->
    request({get_chars,latin1,Prompt,N}, State);
request({get_line,Prompt}, State) ->
    request({get_line,latin1,Prompt}, State);
request({get_until, Prompt,M,F,As}, State) ->
    request({get_until,latin1,Prompt,M,F,As}, State);
request(_Other, State) ->
    {error, {error, request}, State}.

multi_request([R|Rs], {ok, _Res, State}) ->
    multi_request(Rs, request(R, State));
multi_request([_|_], Error) ->
    Error;
multi_request([], Result) ->
    Result.


%% --------------------------------------------------------------
put_chars(Chars, #state{fd_stream = Fd_stream} = State) ->
	journald_api:write_fd(Fd_stream, Chars),
    {ok, ok, State}.

get_until(Encoding, Mod, Func, As, #state{position = P, mode = M, fd = Fd} = State) ->
    case get_loop(Mod, Func, As, Fd, P, []) of
		{done, Data, _, NewP} when is_binary(Data); is_list(Data) ->
		    if
				M =:= binary -> 
				    {ok, 
				     unicode:characters_to_binary(Data, unicode, Encoding),
				     State#state{position = NewP}
				    };
				true ->
				    case check(Encoding, unicode:characters_to_list(Data, unicode)) of
						{error, _} = E ->
						    {error, E, State};
						List ->
						    {ok, List, State#state{position = NewP}}
				    end
		    end;
		{done, Data, _, NewP} ->
		    {ok, Data, State#state{position = NewP}};
		Error ->
		    {error, Error, State}
    end.

get_loop(M,F,A,Fd,P,C) ->
    {NewP,L} = get(P,Fd),
    case catch apply(M,F,[C,L|A]) of
		{done, List, Rest} ->
		    {done, List, [], NewP - length(Rest)};
		{more, NewC} ->
		    get_loop(M,F,A,Fd,NewP,NewC);
		_ ->
		    {error,F}
    end.

get(Pos, Fd) ->
	case journald_api:get_data(Fd, "MESSAGE") of
		{ok, Message} ->
			case journald_api:next(Fd) of
				eaddrnotavail ->
					{Pos, Message};
				ok ->
					{Pos + 1, Message};
				_ ->
					erlang:error(next_failed)
			end;
		_ ->
			erlang:error(get_data_failed)
	end.


setopts(Opts0,State) ->
    Opts = proplists:unfold(proplists:substitute_negations([{list,binary}], Opts0)),
    case check_valid_opts(Opts) of
		true ->
		    case proplists:get_value(binary, Opts) of
			  	true ->
					{ok,ok,State#state{mode=binary}};
				false ->
					{ok,ok,State#state{mode=binary}};
			   	_ ->
					{ok,ok,State}
			end;
		false ->
		    {error,{error,enotsup},State}
    end.

check_valid_opts([]) ->
    true;
check_valid_opts([{binary,Bool}|T]) when is_boolean(Bool) ->
    check_valid_opts(T);
check_valid_opts(_) ->
    false.

getopts(#state{mode=M} = S) ->
    {ok,[{binary, 	case M of
		      			binary ->
			  				true;
		      			_ ->
			  				false
		  			end
		  			}],S}.

check(unicode, List) ->
    List;
check(latin1, List) ->
    try 
		[ throw(not_unicode) || X <- List, X > 255 ],
		List
    catch
		throw:_ ->
	    	{error,{cannot_convert, unicode, latin1}}
    end.

%% --------------------------------------------------------------

get_line(_ThisFar, CharList) ->
	{done, CharList, []}.

get_chars(_ThisFar, CharList, N) when length(CharList) >= N ->
    {Res,_Rest} = lists:split(N, CharList),
    {done,Res,[]};
get_chars(_ThisFar,_CharList,_N) ->
	erlang:error(get_chars_failed).

