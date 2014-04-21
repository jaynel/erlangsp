%%%------------------------------------------------------------------------------
%%% @copyright (c) 2012-2013, DuoMark International, Inc.  All rights reserved
%%% @author Jay Nelson <jay@duomark.com>
%%% @doc
%%%    Receive loop for esp_tcp_service connections.
%%% @since v0.1.0
%%% @end
%%%------------------------------------------------------------------------------
-module(esp_tcp_recv).

-include("license_and_copyright.hrl").
-author('Jay Nelson <jay@duomark.com>').

-type tcp_data() :: binary() | list().
-callback recv        (gen_tcp:socket(), tcp_data()) -> any().
-callback recv_error  (gen_tcp:socket(), any())      -> any().
-callback recv_closed (gen_tcp:socket())             -> any().

%% Public API
-export([active_loop/2, passive_loop/2]).

active_loop(Socket, Handler) ->
    inet:setopts(Socket, [{active, once}]),
    receive
        {tcp, Socket, Data} ->
            Handler:recv(Socket, Data),
            active_loop(Socket, Handler);
        {tcp_error, Socket, Reason} ->
            Handler:recv_error(Socket, Reason),
            active_loop(Socket, Handler);
        {tcp_closed, Socket} ->
            try Handler:recv_closed(Socket)
            catch _:_ -> ok
            end,
            tcp_closed;
        Other ->
            Args = [Socket, Other, self()],
            error_logger:info_msg("Unexpected socket data: ~p ~p ~p~n", Args),
            active_loop(Socket, Handler)
    end.

%% Not implemented yet.
passive_loop(_Socket, _Handler) ->
    ok.
