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
-module(coop_node_ctl_rcv).

-include("../../license_and_copyright.hrl").
-author('Jay Nelson <jay@duomark.com>').

%% Graph API
-export([node_ctl_loop/7]).

-include("coop.hrl").
-include("coop_dag.hrl").
-include("coop_node.hrl").

%%----------------------------------------------------------------------
%% Coop Node data is executed using Node_Fn and the results are
%% passed to one or more of the downstream workers.
%%----------------------------------------------------------------------
-spec node_ctl_loop(pid(), pid(), coop_init_fn(), coop_task_fn(), pid(), pid(), pid()) -> no_return().

node_ctl_loop(Kill_Switch, Task_Pid, Init_Fn, Node_Fn, Trace_Pid, Log_Pid, Reflect_Pid) ->
    node_ctl_loop(#coop_node_state{kill_switch=Kill_Switch, ctl=self(), task=Task_Pid,
                                   init_fn=Init_Fn, task_fn=Node_Fn,
                                   trace=Trace_Pid, log=Log_Pid, reflect=Reflect_Pid}).

node_ctl_loop(#coop_node_state{task=Task_Pid, trace=Trace_Pid} = Coop_Node_State) ->
    receive
        %% Commands for controlling the entire Coop_Node element...
        ?CTL_MSG(stop)    -> exit(stopped);
        ?CTL_MSG(clone)   -> node_clone(Coop_Node_State);

        %% Commands for controlling/monitoring the Task_Pid...
        ?CTL_MSG(suspend) -> sys:suspend(Task_Pid);
        ?CTL_MSG(resume)  -> sys:resume(Task_Pid);
        ?CTL_MSG(trace)   -> erlang:trace(Task_Pid, true,  trace_options(Trace_Pid));
        ?CTL_MSG(untrace) -> erlang:trace(Task_Pid, false, trace_options(Trace_Pid));

        ?CTL_MSG(log,         Flag, {Ref, From}) -> From ! {node_ctl_log, Ref, sys:log(Task_Pid, Flag)};
        ?CTL_MSG(log_to_file, File, {Ref, From}) -> From ! {node_ctl_log_to_file, Ref, sys:log_to_file(Task_Pid, File)};
        ?CTL_MSG(stats,       Flag, {Ref, From}) -> From ! {node_ctl_stats, Ref, sys:statistics(Task_Pid, Flag)};

        ?CTL_MSG(install_trace_fn, FInfo, {Ref, From}) -> From ! {node_ctl_install_trace_fn, Ref, sys:install(Task_Pid, FInfo)};
        ?CTL_MSG(remove_trace_fn,  FInfo, {Ref, From}) -> From ! {node_ctl_remove_trace_fn,  Ref, sys:remove(Task_Pid, FInfo)};

        %% All others are unknown commands, just unqueue them.
        _Skip_Unknown_Msgs                -> do_nothing
    end,
    node_ctl_loop(Coop_Node_State).

node_clone(#coop_node_state{} = _Coop_Node_State) -> ok.
trace_options(Tracer_Pid) -> [{tracer, Tracer_Pid}, send, 'receive', procs, timestamp].
