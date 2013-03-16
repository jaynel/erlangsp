%%%------------------------------------------------------------------------------
%%% @copyright (c) 2012, DuoMark International, Inc.  All rights reserved
%%% @author Jay Nelson <jay@duomark.com>
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%    Default receive loop for coop_node data.
%%% @since v0.0.1
%%% @end
%%%------------------------------------------------------------------------------
-module(coop_node_data_rcv).

-include("../../license_and_copyright.hrl").
-author('Jay Nelson <jay@duomark.com>').

%% Graph API
-export([start_node_data_loop/6]).

%% System message API functions
-export([
         system_continue/3, system_terminate/4, system_code_change/4,
         format_status/2, debug_coop/3
        ]).

-include("coop.hrl").
-include("coop_dag.hrl").


%%----------------------------------------------------------------------
%% Co-op Node data is executed using Node_Fn and the results are
%% passed to one or more of the downstream workers.
%%----------------------------------------------------------------------
-spec start_node_data_loop(coop_instance(), coop_task_fn(), coop_init_fn(),
                           downstream_workers(), coop_data_options(), data_flow_method()) -> no_return().
-spec node_data_loop(coop_instance(), coop_task_fn(), any(), downstream_workers(),
                     #coop_node_options{}, data_flow_method(), [sys:dbg_opt()]) -> no_return().

init_data_options(Options) ->
    case proplists:get_value(access_coop, Options) of
        undefined -> #coop_node_options{};
        head      -> #coop_node_options{access_coop = head};
        instance  -> #coop_node_options{access_coop = instance}
    end.

start_node_data_loop(Coop_Inst, Node_Fn, {Mod, Fun, Args} = _Init_Fn, Downstream_Pids, Options, Data_Flow_Method) ->
    Data_Opts = init_data_options(Options),
    Init_State = case Data_Opts#coop_node_options.access_coop of
                     none     -> Mod:Fun(Args);
                     instance -> Mod:Fun({Coop_Inst, Args});
                     head     -> Mod:Fun({coop:head(Coop_Inst), Args})
                 end,
    node_data_loop(Coop_Inst, Node_Fn, Init_State, Downstream_Pids, Data_Opts, Data_Flow_Method, sys:debug_options([])).

node_data_loop(Coop_Inst, Node_Fn, Node_State, Downstream_Pids, Data_Opts, Data_Flow_Method, Debug_Opts) ->
    receive
        %% System messages (TODO: fix pattern matching on 'EXIT' to match only true parent)
        {'EXIT', _Parent, Reason} -> exit(Reason);
        {system, From, System_Msg} ->
            Sys_Args = {Coop_Inst, Node_Fn, Node_State, Downstream_Pids, Data_Opts, Data_Flow_Method, Debug_Opts},
            handle_sys(Sys_Args, From, System_Msg);
        {get_modules, From} ->
            {Task_Module, _Task_Fn} = Node_Fn,
            From ! {modules, [?MODULE, Task_Module]},
            node_data_loop(Coop_Inst, Node_Fn, Node_State, Downstream_Pids, Data_Opts, Data_Flow_Method, Debug_Opts);

        %% Node control messages affecting Node_Fn, Pids or Data_Flow_Method...
        ?CTL_MSG(Dag_Ctl_Msg) ->
            New_Debug_Opts = sys:handle_debug(Debug_Opts, fun debug_coop/3, {Data_Flow_Method, Data_Opts, Node_State}, {in, Dag_Ctl_Msg}),
            {Final_Debug_Opts, New_Coop_Inst, New_Downstream_Pids} = handle_ctl(New_Debug_Opts, Coop_Inst, Downstream_Pids, Data_Opts, Data_Flow_Method, Dag_Ctl_Msg),
            node_data_loop(New_Coop_Inst, Node_Fn, Node_State, New_Downstream_Pids, Data_Opts, Data_Flow_Method, Final_Debug_Opts);

        %% All data is passed as is and untagged for processing.
        Data ->
            New_Debug_Opts = sys:handle_debug(Debug_Opts, fun debug_coop/3, {Data_Flow_Method, Data_Opts, Node_State}, {in, Data}),
            {Final_Debug_Opts, Maybe_Reordered_Pids, New_Node_State}
                = relay_data(Coop_Inst, New_Debug_Opts, Node_Fn, Node_State, Data_Opts, Data_Flow_Method, Data, Downstream_Pids),
            node_data_loop(Coop_Inst, Node_Fn, New_Node_State, Maybe_Reordered_Pids, Data_Opts, Data_Flow_Method, Final_Debug_Opts)
    end.

call_task_fn(Mod, Fn, Node_State, Data, none,     _Coop_Inst) -> Mod:Fn(Node_State, Data);
call_task_fn(Mod, Fn, Node_State, Data, head,      Coop_Inst) -> Mod:Fn(coop:head(Coop_Inst), Node_State, Data);
call_task_fn(Mod, Fn, Node_State, Data, instance,  Coop_Inst) -> Mod:Fn(Coop_Inst, Node_State, Data).
    
%% No Downstream_Pids...
relay_data(Coop_Inst, Debug_Opts, {Module, Function} = _Node_Fn, Node_State,
           #coop_node_options{access_coop=Coop_Type}, _Any_Data_Flow_Method, Data, Worker_Set)
  when Worker_Set =:= {}; Worker_Set =:= {[],[]} ->
    {New_Node_State, _Fn_Result} = call_task_fn(Module, Function, Node_State, Data, Coop_Type, Coop_Inst), %% For side effects only.
    {Debug_Opts, Worker_Set, New_Node_State};

%% Faster routing if only one Downstream_Pid...
relay_data(Coop_Inst, Debug_Opts, Node_Fn, Node_State, Data_Opts, Any_Data_Flow_Method, Data, {Pid} = Worker_Set) ->
    notify_debug_and_return(Coop_Inst, Debug_Opts, Node_Fn, Node_State, Data_Opts, Any_Data_Flow_Method, Data, Worker_Set, Pid);

%% Relay data to all Downstream_Pids...
relay_data(Coop_Inst, Debug_Opts, {Module, Function} = _Node_Fn, Node_State,
           #coop_node_options{access_coop=Coop_Type} = Data_Opts, broadcast, Data, Worker_Set) ->
    {New_Node_State, Fn_Result} = call_task_fn(Module, Function, Node_State, Data, Coop_Type, Coop_Inst),
    New_Opts = case Fn_Result of
                   ?COOP_NOOP -> Debug_Opts;
                   Live_Data  ->
                       Relay_Fn = fun(To, Opts) ->
                                          coop:relay_data(To, Live_Data),
                                          Debug_Args = {broadcast, Data_Opts, New_Node_State},
                                          sys:handle_debug(Opts, fun debug_coop/3, Debug_Args, {out, Live_Data, To})
                                  end,
                       lists:foldl(Relay_Fn, Debug_Opts, queue:to_list(Worker_Set))  %% TODO: is this expensive?!
               end,
    {New_Opts, Worker_Set, New_Node_State};

%% Relay data with random or round_robin has to choose a single destination.
relay_data(Coop_Inst, Debug_Opts, Node_Fn, Node_State, Data_Opts, Single_Data_Flow_Method, Data, Worker_Set) ->
    {Worker, New_Worker_Set} = choose_worker(Worker_Set, Single_Data_Flow_Method),
    notify_debug_and_return(Coop_Inst, Debug_Opts, Node_Fn, Node_State, Data_Opts, Single_Data_Flow_Method, Data, New_Worker_Set, Worker).

%% Used only for single downstream pid delivery methods.
notify_debug_and_return(Coop_Inst, Debug_Opts, {Module, Function}, Node_State,
                        #coop_node_options{access_coop=Coop_Type} = Data_Opts, Data_Flow_Method, Data, Worker_Set, Pid) ->
    {New_Node_State, Fn_Result} = call_task_fn(Module, Function, Node_State, Data, Coop_Type, Coop_Inst),
    New_Opts = case Fn_Result of
                   ?COOP_NOOP -> Debug_Opts;
                   Live_Data  -> coop:relay_data(Pid, Live_Data),
                                 Debug_Args = {Data_Flow_Method, Data_Opts, New_Node_State},
                                 sys:handle_debug(Debug_Opts, fun debug_coop/3, Debug_Args, {out, Live_Data, Pid})
    end,
    {New_Opts, Worker_Set, New_Node_State}.

%% Choose a worker randomly without changing the Worker_Set...
choose_worker(Worker_Set, random) ->
    N = coop_node_util:random_worker(Worker_Set),
    {element(N, Worker_Set), Worker_Set};
%% Grab first worker, then rotate worker list for round_robin.
choose_worker(Worker_Set, round_robin) ->
    {{value, Worker}, Set_Minus_Worker} = queue:out(Worker_Set),
    {Worker, queue:in(Worker, Set_Minus_Worker)}.


%%----------------------------------------------------------------------
%% Control message requests affecting data receive loop
%%----------------------------------------------------------------------
handle_ctl(Debug_Opts, _Coop_Inst, Downstream_Pids, _Data_Opts, _Data_Flow_Method, {replace_coop_instance, New_Coop_Inst} = Msg) ->
    New_Debug_Opts = relay_ctl(Debug_Opts, Downstream_Pids, Msg),
    {New_Debug_Opts, New_Coop_Inst, Downstream_Pids};
handle_ctl(Debug_Opts,  Coop_Inst, Downstream_Pids, _Data_Opts, _Data_Flow_Method, {get_coop_instance, {Ref, From}}) ->
    From ! {get_coop_head, Ref, Coop_Inst},
    {Debug_Opts, Coop_Inst, Downstream_Pids};
handle_ctl(Debug_Opts,  Coop_Inst, Downstream_Pids, _Data_Opts, _Data_Flow_Method, {get_coop_head, {Ref, From}}) ->
    From ! {get_coop_head, Ref, coop:head(Coop_Inst)},
    {Debug_Opts, Coop_Inst, Downstream_Pids};
handle_ctl(Debug_Opts,  Coop_Inst, Downstream_Pids, _Data_Opts, _Data_Flow_Method, {add_downstream, []}) ->
    {Debug_Opts, Coop_Inst, Downstream_Pids};
handle_ctl(Debug_Opts,  Coop_Inst, Downstream_Pids, _Data_Opts,  Data_Flow_Method, {add_downstream, New_Pids})
  when is_list(New_Pids) ->
    {Debug_Opts, Coop_Inst, do_add_downstream(Data_Flow_Method, Downstream_Pids, New_Pids)};
handle_ctl(Debug_Opts,  Coop_Inst, Downstream_Pids, _Data_Opts, _Data_Flow_Method, {remove_downstream, []}) ->
    {Debug_Opts, Coop_Inst, Downstream_Pids};
handle_ctl(Debug_Opts,  Coop_Inst, Downstream_Pids, _Data_Opts,  Data_Flow_Method, {remove_downstream, Old_Pids})
  when is_list(Old_Pids) ->
    {Debug_Opts, Coop_Inst, do_remove_downstream(Data_Flow_Method, Downstream_Pids, Old_Pids)};
handle_ctl(Debug_Opts,  Coop_Inst, Downstream_Pids, _Data_Opts,  Data_Flow_Method, {get_downstream, {Ref, From}}) ->
    reply_downstream_pids_as_list(Data_Flow_Method, Downstream_Pids, Ref, From),
    {Debug_Opts, Coop_Inst, Downstream_Pids};
handle_ctl(Debug_Opts,  Coop_Inst, Downstream_Pids, _Data_Opts, _Data_Flow_Method, _Unknown_Cmd) ->
    error_logger:info_msg("~p Unknown DAG Cmd: ~p~n", [?MODULE, _Unknown_Cmd]),
    {Debug_Opts, Coop_Inst, Downstream_Pids}.

do_add_downstream(random, Downstream_Pids, New_Pids) ->
    list_to_tuple(tuple_to_list(Downstream_Pids) ++ New_Pids);

do_add_downstream(_Not_Random, {},     [Pid])    -> {Pid};
do_add_downstream(_Not_Random, {},     New_Pids) -> queue:from_list(New_Pids);
do_add_downstream(_Not_Random, {Pid},  New_Pids) -> queue:from_list([Pid | New_Pids]);
do_add_downstream(_Not_Random, Downstream_Pid_Queue, New_Pids) ->
    queue:join(Downstream_Pid_Queue, queue:from_list(New_Pids)).

do_remove_downstream(random, Downstream_Pids, Old_Pids) ->
    list_to_tuple(tuple_to_list(Downstream_Pids) -- Old_Pids);

do_remove_downstream(_Not_Random, {},    _Old_Pids) -> {};
do_remove_downstream(_Not_Random, {Pid},  Old_Pids) ->
    case lists:member(Pid, Old_Pids) of
        true  -> {};
        false -> {Pid}
    end;
do_remove_downstream(_Not_Random, Downstream_Pid_Queue, New_Pids) ->
    queue:from_list(queue:to_list(Downstream_Pid_Queue) -- New_Pids).

reply_downstream_pids_as_list(random, Downstream_Pids, Ref, From) ->
    From ! {get_downstream, Ref, tuple_to_list(Downstream_Pids)};
reply_downstream_pids_as_list(_Not_Random, Downstream_Pids, Ref, From) ->
    case Downstream_Pids of
        {}    -> From ! {get_downstream, Ref, []};
        {Pid} -> From ! {get_downstream, Ref, [Pid]};
        Queue -> From ! {get_downstream, Ref, queue:to_list(Queue)}
    end.

    
%% No Downstream_Pids...
relay_ctl(Debug_Opts, Downstream_Pids, _Msg)
  when Downstream_Pids =:= {}; Downstream_Pids =:= {[],[]} ->
    Debug_Opts;

%% Faster routing if only one Downstream_Pid...
relay_ctl(Debug_Opts, {Downstream_Pid}, Msg) ->
    coop:relay_data(Downstream_Pid, Msg),
    Debug_Args = {ctl_flow, [], Msg},
    sys:handle_debug(Debug_Opts, fun debug_coop/3, Debug_Args, {out, Msg, Downstream_Pid});

%% Relay data to all Downstream_Pids even if not broadcast flow type.
relay_ctl(Debug_Opts, Downstream_Pids, Msg) ->
    Relay_Fn = fun(To, Opts) ->
                       coop:relay_data(To, Msg),
                       Debug_Args = {ctl_flow, [], Msg},
                       sys:handle_debug(Opts, fun debug_coop/3, Debug_Args, {out, Msg, To})
               end,
    lists:foldl(Relay_Fn, Debug_Opts, queue:to_list(Downstream_Pids)).  %% TODO: is this expensive?!


%%----------------------------------------------------------------------
%% System, debug and control messages for OTP compatibility
%%----------------------------------------------------------------------
-spec system_continue(pid(), [sys:dbg_opt()], term()) -> no_return().
-spec system_terminate(atom(), pid(), [sys:dbg_opt()], term()) -> no_return().
-spec system_code_change(term(), module(), atom(), term()) -> {ok, term()}.
-spec format_status(normal | terminate, list()) -> [proplists:property()].

handle_sys({_Coop_Inst, _Node_Fn, _Node_State, _Downstream_Pids, _Data_Opts, _Data_Flow_Method, Debug_Opts} = Coop_Internals,
           From, System_Msg) ->
    [Parent | _] = get('$ancestors'),
    sys:handle_system_msg(System_Msg, From, Parent, ?MODULE, Debug_Opts, Coop_Internals).

debug_coop(Dev, Event, State) ->
    io:format(Dev, "~p DBG: ~p event = ~p~n", [self(), State, Event]).

system_continue(_Parent, New_Debug_Opts,
                {Coop_Inst, Node_Fn, Node_State, Downstream_Pids, Data_Opts, Data_Flow_Method, _Old_Debug_Opts} = _Misc) ->
    node_data_loop(Coop_Inst, Node_Fn, Node_State, Downstream_Pids, Data_Opts, Data_Flow_Method, New_Debug_Opts).

system_terminate(Reason, _Parent, _Debug_Opts, _Misc) -> exit(Reason).
system_code_change(Misc, _Module, _OldVsn, _Extra) -> {ok, Misc}.

format_status(normal, [_PDict, Sys_State, Parent, New_Debug_Opts,
                       {_Coop_Inst, Node_Fn, Node_State, Downstream_Pids, Data_Opts, Data_Flow_Method, _Old_Debug_Opts}]) ->
    Pid_Count = case Data_Flow_Method of
                    random -> tuple_size(Downstream_Pids);
                    _Not_Random ->
                        case Downstream_Pids of
                            {}    -> 0;
                            {_Pid} -> 1;
                            Queue -> queue:len(Queue)
                        end
                end,
    Hdr = "Status for coop_node",
    Log = sys:get_debug(log, New_Debug_Opts, []),
    [{header, Hdr},
     {data, [{"Status",               Sys_State},
             {"Node_Fn",              Node_Fn},
             {"Node_State",           Node_State},
             {"Downstream_Pid_Count", Pid_Count},
             {"Data_Options",         format_data_options(Data_Opts)},
             {"Data_Flow_Method",     Data_Flow_Method},
             {"Parent",               Parent},
             {"Logged events",        Log},
             {"Debug",                New_Debug_Opts}]
     }];

format_status(terminate, Status_Data) -> [{terminate, Status_Data}].

format_data_options(#coop_node_options{access_coop=Coop_Type}) ->
    "{access_coop: " ++ atom_to_list(Coop_Type) ++ "}".
