%%%------------------------------------------------------------------------------
%%% @copyright (c) 2012, DuoMark International, Inc.  All rights reserved
%%% @author Jay Nelson <jay@duomark.com>
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%    Coop Head data process receive loop.
%%% @since v0.0.1
%%% @end
%%%------------------------------------------------------------------------------
-module(coop_head_data_rcv).

-include("../../license_and_copyright.hrl").
-author('Jay Nelson <jay@duomark.com>').

%% Receive loop methods
-export([one_at_a_time_loop/2, relay_msg_to_root_pid/3]).

%% System message API functions
-export([
         system_continue/3, system_terminate/4, system_code_change/4,
         format_status/2, debug_coop/3
        ]).

-include("coop.hrl").
-include("coop_dag.hrl").


-spec one_at_a_time_loop(pid(), pos_integer() | none) -> no_return().
            
%% Exit, initialize, timeout changes and getting the root_pid don't need root_pid involvement...
one_at_a_time_loop(Root_Pid, Timeout) ->
    one_at_a_time_loop(Root_Pid, Timeout, sys:debug_options([])).

%% One-at-a-time sends one synchronous message (waits for the ack) before the next.
one_at_a_time_loop(Root_Pid, Timeout, Debug_Opts) ->
    receive

        %% System messages for compatibility with OTP...
        {'EXIT', _Parent, Reason}  -> exit(Reason);
        {system, From, System_Msg} ->
            Sys_Args = {{}, Root_Pid, Timeout, Debug_Opts},
            handle_sys(Sys_Args, From, System_Msg);
        {get_modules, From} ->
            From ! {modules, [?MODULE]},
            one_at_a_time_loop(Root_Pid, Timeout, Debug_Opts);

        %% Ctl tag is used for meta-data about the data process...
        ?CTL_MSG({change_timeout, New_Timeout}) ->
            one_at_a_time_loop(Root_Pid, New_Timeout, Debug_Opts);

        %% Data must be tagged as such to be processed...
        ?DATA_MSG(Data_Msg) ->
            ack = relay_msg_to_root_pid(Data_Msg, Root_Pid, Timeout),
            one_at_a_time_loop(Root_Pid, Timeout, Debug_Opts);

        %% Quit if random data shows up.
        Unexpected ->
            error_logger:error_msg("Unexpected ~p ~p~n", [?MODULE, Unexpected]),
            exit({coop_head_bad_data, Unexpected})
    end.

relay_msg_to_root_pid(Msg, Root_Pid, Timeout) ->
    
    %% New ref causes selective receive optimization when looking for ACK.
    Ref = make_ref(),
    Root_Pid ! {?DATA_TOKEN, {Ref, self()}, Msg},

    %% Wait for Root_Pid to ack the receipt of data.
    case Timeout of
        none -> receive {?ROOT_TOKEN, Ref, Root_Pid} -> ack end;
        Milliseconds when is_integer(Milliseconds), Milliseconds > 0 ->
            receive {?ROOT_TOKEN, Ref, Root_Pid} -> ack
            after Timeout -> exit({coop_root_ack_timeout, Msg})
            end
    end.


%%----------------------------------------------------------------------
%% System, debug and control messages for OTP compatibility
%%----------------------------------------------------------------------
-spec system_continue(pid(), [sys:dbg_opt()], term()) -> no_return().
-spec system_terminate(atom(), pid(), [sys:dbg_opt()], term()) -> no_return().
-spec system_code_change(term(), module(), atom(), term()) -> {ok, term()}.
-spec format_status(normal | terminate, list()) -> [proplists:property()].

handle_sys({_State, _Root_Pid, _Timeout, Debug_Opts} = Ctl_Internals, From, System_Msg) ->
    [Parent | _] = get('$ancestors'),
    sys:handle_system_msg(System_Msg, From, Parent, ?MODULE, Debug_Opts, Ctl_Internals).

debug_coop(Dev, Event, State) ->
    io:format(Dev, "DBG: ~p event = ~p~n", [State, Event]).

system_continue(_Parent, Debug_Opts, {_State, Root_Pid, Timeout, _Old_Debug_Opts} = _Misc) ->
    one_at_a_time_loop(Root_Pid, Timeout, Debug_Opts).

system_terminate(Reason, _Parent, _Debug_Opts, _Misc) -> exit(Reason).
system_code_change(Misc, _Module, _OldVsn, _Extra) -> {ok, Misc}.

format_status(normal, [_PDict, Sys_State, Parent, New_Debug_Opts,
                       {_State, _Root_Pid, _Timeout, _Old_Debug_Opts}]) ->
    Hdr = "Status for " ++ atom_to_list(?MODULE),
    Log = sys:get_debug(log, New_Debug_Opts, []),
    [{header, Hdr},
     {data, [{"Status",          Sys_State},
             {"Parent",          Parent},
             {"Logged events",   Log},
             {"Debug",           New_Debug_Opts}
            ]
     }];

format_status(terminate, Status_Data) -> [{terminate, Status_Data}].
