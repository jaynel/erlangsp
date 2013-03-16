%%%------------------------------------------------------------------------------
%%% @copyright (c) 2012, DuoMark International, Inc.  All rights reserved
%%% @author Jay Nelson <jay@duomark.com>
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%    Coop Head control process receive loop.
%%% @since v0.0.1
%%% @end
%%%------------------------------------------------------------------------------
-module(coop_head_ctl_rcv).

-include("../../license_and_copyright.hrl").
-author('Jay Nelson <jay@duomark.com>').

%% Receive loop methods
-export([msg_loop/3]).

%% System message API functions
-export([
         system_continue/3, system_terminate/4, system_code_change/4,
         format_status/2, debug_coop/3
        ]).

-include("coop.hrl").
-include("coop_dag.hrl").
-include("coop_head.hrl").

%% Exit, initialize, timeout changes and getting the root_pid don't need root_pid involvement...
msg_loop(State, Root_Pid, Timeout) ->
    msg_loop(State, Root_Pid, Timeout, sys:debug_options([])).


%% Macros to make msg_loop patterns shorter and more apparent.
-define(MSG_LOOP_RECURSE,              msg_loop(State,       Root_Pid, Timeout, Debug_Opts)).
-define(MSG_LOOP_RECURSE(__New_State), msg_loop(__New_State, Root_Pid, Timeout, Debug_Opts)).

%% Until init finished, respond only to OTP and receiving an initial state record.
msg_loop({} = State, Root_Pid, Timeout, Debug_Opts) ->
    receive
        %% System messages for compatibility with OTP...
        {'EXIT', _Parent, Reason}  -> exit(Reason);
        {system, From, System_Msg} ->
            Sys_Args = {State, Root_Pid, Timeout, Debug_Opts},
            handle_sys(Sys_Args, From, System_Msg);
        {get_modules, From}        ->
            From ! {modules, [?MODULE]},
            ?MSG_LOOP_RECURSE;
        ?CTL_MSG({init_state, #coop_head_state{} = New_State}) ->
            ?MSG_LOOP_RECURSE(New_State)
    end;


%% Normal message loop after initial state is received.
msg_loop(#coop_head_state{} = State, Root_Pid, Timeout, Debug_Opts) ->
    receive
        %% System messages for compatibility with OTP...
        {'EXIT', _Parent, Reason}  -> exit(Reason);
        {get_modules, From}        -> From ! {modules, [?MODULE]}, ?MSG_LOOP_RECURSE;
        {system, From, System_Msg} -> Sys_Args = {State, Root_Pid, Timeout, Debug_Opts}, handle_sys(Sys_Args, From, System_Msg);

        %% Coop System message control messages...
        ?CTL_MSG(stop)    -> exit(stopped);
        ?CTL_MSG(resume)  -> sys:resume(Root_Pid),  ?MSG_LOOP_RECURSE;
        ?CTL_MSG(suspend) -> sys:suspend(Root_Pid), ?MSG_LOOP_RECURSE;

        ?CTL_MSG(format_status)                   -> State#coop_head_state.log ! sys:get_status(Root_Pid),             ?MSG_LOOP_RECURSE;
        ?CTL_MSG(stats, Flag, {Ref, From})        -> From ! {stats, Ref, sys:statistics(Root_Pid, Flag)},              ?MSG_LOOP_RECURSE;
        ?CTL_MSG(log,   Flag, {Ref, From})        -> From ! {log, Ref, sys:log(Root_Pid, Flag)},                       ?MSG_LOOP_RECURSE;
        ?CTL_MSG(log_to_file, File, {Ref, From})  -> From ! {log_to_file, Ref, sys:log_to_file(Root_Pid, File)},       ?MSG_LOOP_RECURSE;

        %% State management and access control messages...
        ?CTL_MSG({change_timeout, New_Timeout})   -> msg_loop(State, Root_Pid, New_Timeout, Debug_Opts);
        ?CTL_MSG({get_kill_switch, {Ref, From}})  -> From ! {get_kill_switch, Ref, State#coop_head_state.kill_switch}, ?MSG_LOOP_RECURSE;
        ?CTL_MSG({get_root_pid, {Ref, From}})     -> From ! {get_root_pid, Ref, Root_Pid},                             ?MSG_LOOP_RECURSE;
        ?CTL_MSG({set_root_node, #coop_node{} = Coop_Node, {Ref, From}} = Msg) ->
            case State#coop_head_state.coop_root_node of
                none ->
                    Root_Pid ! {?CTL_TOKEN, Msg},
                    New_State = State#coop_head_state{coop_root_node=Coop_Node},
                    ?MSG_LOOP_RECURSE(New_State);
                _Already_Set ->
                    From ! {set_root_node, Ref, false},
                    ?MSG_LOOP_RECURSE
            end;

        %% Priority data messages bypass data queue via control channel,
        %% but can clog control processing waiting for ACKs. The timeout
        %% used is relatively short, and backlog can cause the Coop to
        %% crash, so high priority data should be sent sparingly.
        ?DATA_MSG(Data_Msg) -> ack = coop_head_data_rcv:relay_msg_to_root_pid(Data_Msg, Root_Pid, Timeout), ?MSG_LOOP_RECURSE;

        %% Unrecognized control msgs are forwarded to Root Pid, without regard to how
        %% many messages are currently pending on the Root Pid queue. Sending too many
        %% can cause a high priority data message to crash the entire Coop.
        ?CTL_MSG(Ctl_Msg)   -> Root_Pid ! {?CTL_TOKEN, Ctl_Msg}, ?MSG_LOOP_RECURSE;

        %% Quit if random data shows up.
        _Unexpected ->
            exit(coop_head_bad_ctl)
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

system_continue(_Parent, Debug_Opts, {State, Root_Pid, Timeout, _Old_Debug_Opts} = _Misc) ->
    ?MSG_LOOP_RECURSE.

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
