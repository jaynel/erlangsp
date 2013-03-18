%%%------------------------------------------------------------------------------
%%% @copyright (c) 2012, DuoMark International, Inc.  All rights reserved
%%% @author Jay Nelson <jay@duomark.com>
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%    Co-operating Process instances modeled on coop_flow graphs.
%%% @since v0.0.1
%%% @end
%%%------------------------------------------------------------------------------
-module(coop).

-include("../../license_and_copyright.hrl").
-author('Jay Nelson <jay@duomark.com>').

%% External API
-export([
         new_pipeline/2, new_fanout/3, migrate_fanout_node/3,
         make_dag_node/3, make_dag_node/4, make_dag_node/5,
         make_dag_nodes/4, make_dag_nodes/5, make_dag_nodes/6,
         get_head_kill_switch/1, get_body_kill_switch/1,
         head/1, is_live/1, relay_data/2, relay_high_priority_data/2
        ]).

%% For testing purposes only.
-export([pipeline/4, fanout/4]).

-include("coop.hrl").
    

%%----------------------------------------------------------------------
%% Create a Co-op (with a single Co-op Instance)
%%
%% PIPELINE:
%%   A chain of Co-op Nodes, ordered left to right. Data is
%%   received at the left Node, transformed at each Node and emerges
%%   normally at the right Node ending in transfer to the Co-op
%%   Receiver.
%%
%% FANOUT:
%%   A single Co-op Node which has 2 or more immediate downstream
%%   Nodes. Data must flow through the single entry point, but may
%%   then be directed to any one or more of the children Nodes.
%%----------------------------------------------------------------------
-spec new_pipeline([#coop_dag_node{}], coop_receiver()) -> coop() | false.
-spec new_fanout(#coop_dag_node{}, [#coop_dag_node{}], coop_receiver()) -> coop() | false.

new_pipeline([#coop_dag_node{} | _More] = Node_Fns, Receiver) ->
    {Coop_Head, Body_Kill_Switch} = coop_head_setup(),
    Pipeline_Template_Graph = coop_flow:pipeline(Node_Fns),
    Coop_Instance = make_coop_instance(1, Coop_Head, none, none),
    Vertex_List = [digraph:vertex(Pipeline_Template_Graph, Name)
                   || #coop_dag_node{name=Name} <- Node_Fns],
    Final_Coop_Instance
        = pipeline(Coop_Instance, Body_Kill_Switch, Vertex_List, Receiver),
    finalize_coop_instance_in_data_loop(Coop_Instance),
    #coop{instances=Final_Coop_Instance, dataflow=broadcast, dag_template=Pipeline_Template_Graph}.
    
new_fanout(#coop_dag_node{name=Inbound} = Router_Fn, Workers, Receiver) ->
    {Coop_Head, Body_Kill_Switch} = coop_head_setup(),
    Fanout_Template_Graph = coop_flow:fanout(Router_Fn, Workers, Receiver),
    Coop_Instance = make_coop_instance(1, Coop_Head, none, none),
    Final_Coop_Instance = fanout(Inbound, Coop_Instance, Body_Kill_Switch, Fanout_Template_Graph),
    finalize_coop_instance_in_data_loop(Coop_Instance),
    #coop{instances=Final_Coop_Instance, dataflow=broadcast, dag_template=Fanout_Template_Graph}.

coop_head_setup() ->
    Head_Kill_Switch = coop_kill_link_rcv:make_kill_switch(),
    Body_Kill_Switch = coop_kill_link_rcv:make_kill_switch(),
    Coop_Head = coop_head:new(Head_Kill_Switch, none),
    {Coop_Head, Body_Kill_Switch}.
    
%% TODO: This currently ignores maintaining the template graph
migrate_fanout_node(#coop_node{ctl_pid=Worker_Ctl_Pid, task_pid=Worker_Task_Pid}  = Worker,
                    #coop_instance{head=Coop_From_Head, body=Coop_From_Root_Node} = _Coop_From,
                    #coop_instance{head=Coop_To_Head,   body=Coop_To_Root_Node}   = _Coop_To) ->

    From_Head_Kill_Switch = get_head_kill_switch(Coop_From_Head),
    From_Body_Kill_Switch = get_body_kill_switch(Coop_From_Root_Node),

    To_Head_Kill_Switch   = get_head_kill_switch(Coop_To_Head),
    To_Body_Kill_Switch   = get_body_kill_switch(Coop_To_Root_Node),
    
    Linked_Pids = [Worker_Ctl_Pid, Worker_Task_Pid],
    ct:log("Linked Pids: ~p~n", [Linked_Pids]),
    coop_kill_link_rcv:unlink_from_kill_switch(From_Head_Kill_Switch, Linked_Pids),
    coop_kill_link_rcv:unlink_from_kill_switch(From_Body_Kill_Switch, Linked_Pids),
    coop_node:node_task_remove_downstream_pids(Coop_From_Root_Node, [Worker]),

    coop_kill_link_rcv:link_to_kill_switch(To_Head_Kill_Switch, Linked_Pids),
    coop_kill_link_rcv:link_to_kill_switch(To_Body_Kill_Switch, Linked_Pids),
    coop_node:node_task_add_downstream_pids(Coop_To_Root_Node, [Worker]).


%%----------------------------------------------------------------------
%% Functions for making Co-op records
%%----------------------------------------------------------------------
-spec make_coop_instance(integer(), coop_head(), coop_body() | none, digraph() | none) -> coop_instance() | {error, invalid_head_or_body}.
-spec make_dag_node(string() | atom(), coop_init_fn(), coop_task_fn(), coop_data_options()) -> coop_dag_node().
-spec make_dag_node(string() | atom(), coop_init_fn(), coop_task_fn(), coop_data_options(), data_flow_method()) -> coop_dag_node() | {error, {invalid_data_flow_method, any()}}.

make_coop_instance(Id, #coop_head{} = Head, none = Body, Dag)
  when is_integer(Id), Id > 0 ->
    #coop_instance{id=Id, head=Head, body=Body, dag=Dag};
make_coop_instance(_Id, _Head, none = _Body, _Dag) ->
    {error, invalid_coop_head}.

set_coop_root_node(#coop_instance{head=Head} = Instance, #coop_node{} = Root_Node) ->
    coop_head:set_root_node(Head, Root_Node),
    Instance.

make_dag_node(Init_Fn, Task_Fn, Opts) ->
    make_dag_node(undefined, Init_Fn, Task_Fn, Opts).
    
make_dag_node(Name, Init_Fn, Task_Fn, Opts) ->
    make_dag_node(Name, Init_Fn, Task_Fn, Opts, broadcast).

make_dag_node(Name, {_Imod, _Ifun, _Iargs} = Init_Fn, {_Mod, _Fun} = Task_Fn, Opts, Data_Flow)
  when is_atom(_Imod), is_atom(_Ifun), is_atom(_Mod), is_atom(_Fun), is_list(Opts) ->
    case lists:member(Data_Flow, ?DATAFLOW_TYPES) of
        true  ->
            Label = #coop_node_fn{init=Init_Fn, task=Task_Fn, options=Opts, flow=Data_Flow},
            #coop_dag_node{name=Name, label=Label};
        false -> {error, {invalid_data_flow_method, Data_Flow}}
    end.

make_dag_nodes(N, Init_Fn, Task_Fn, Opts) ->
    make_dag_nodes([], N, undefined, Init_Fn, Task_Fn, Opts, broadcast).

make_dag_nodes(N, Name, Init_Fn, Task_Fn, Opts) when is_list(Name); Name =:= undefined ->
    make_dag_nodes([], N, Name, Init_Fn, Task_Fn, Opts, broadcast).
    
make_dag_nodes(N, Name, Init_Fn, Task_Fn, Opts, Data_Flow) when is_list(Name); Name =:= undefined ->
    make_dag_nodes([], N, Name, Init_Fn, Task_Fn, Opts, Data_Flow).

make_dag_nodes([], N, Name, Init_Fn, Task_Fn, Opts, Data_Flow)
  when is_integer(N), is_list(Opts) ->
    case lists:member(Data_Flow, ?DATAFLOW_TYPES) of
        false -> {error, {invalid_data_flow_method, Data_Flow}};
        true  -> Label = #coop_node_fn{init=Init_Fn, task=Task_Fn, options=Opts, flow=Data_Flow},
                 make_n_dag_nodes([], N, Name, Label)
    end.

make_n_dag_nodes(New_Nodes, 0, _Name,     _Label) -> New_Nodes;
make_n_dag_nodes(New_Nodes, N, undefined,  Label) ->
    make_n_dag_nodes([#coop_dag_node{label=Label} | New_Nodes], N-1, undefined, Label);
make_n_dag_nodes(New_Nodes, N, Name,       Label) ->
    Name_Str = Name ++ "-" ++ integer_to_list(N),
    make_n_dag_nodes([#coop_dag_node{name=Name_Str, label=Label} | New_Nodes], N-1, undefined, Label).
    
    
%%----------------------------------------------------------------------
%% Get a reference to the Co-op Head or Co-op Body kill switch Pid.
%%----------------------------------------------------------------------
get_head_kill_switch(Coop_Head) ->
    coop_head:get_kill_switch(Coop_Head).

get_body_kill_switch(Coop_Node) ->
    coop_node:get_kill_switch(Coop_Node).


%%----------------------------------------------------------------------
%% Check if a Coop_Head, Coop_Node or raw Pid is alive.
%%----------------------------------------------------------------------
-spec head(coop()) -> coop_head().
-spec is_live(none | pid() | coop_head() | coop_node() | coop_instance() | coop()) -> boolean().

head(#coop{instances=#coop_instance{head=#coop_head{} = Coop_Head}}) -> Coop_Head;
head(#coop{instances=_Ets_Table}) -> not_implemented_yet.
    
is_live(none) -> false;
is_live(Pid) when is_pid(Pid) -> is_process_alive(Pid);
is_live(#coop_head{ctl_pid=Ctl_Pid, data_pid=Data_Pid}) ->
    is_process_alive(Ctl_Pid) andalso is_process_alive(Data_Pid);
is_live(#coop_node{ctl_pid=Ctl_Pid, task_pid=Task_Pid}) ->
    is_process_alive(Ctl_Pid) andalso is_process_alive(Task_Pid);
is_live(#coop_instance{head=Coop_Head}) ->
    is_live(Coop_Head);
is_live(#coop{instances=#coop_instance{head=Coop_Head}}) ->
    is_live(Coop_Head);
is_live(#coop{instances=Ets_Table}) ->
    ets:foldl(fun(#coop_instance{} = Inst, true) -> is_live(Inst);
                 (_Any, false) -> false end, true, Ets_Table).
    


%%----------------------------------------------------------------------
%% Ctl messaging to Co-op Instances
%%----------------------------------------------------------------------

%% Finalize coop_instance by replacing the reference carried inside
%% coop_node_data_rcv loops for all coop_nodes. This must be done
%% last, but before data is received, due to circular references.
finalize_coop_instance_in_data_loop(#coop_instance{head=Coop_Head} = Coop_Instance) ->
    coop_head:send_ctl_msg(Coop_Head, {replace_coop_instance, Coop_Instance}),
    ok.


%%----------------------------------------------------------------------
%% Data messaging to Co-op Instances
%%----------------------------------------------------------------------

%% Relay data is used to deliver Node output to Co-op, Co-op Instance,
%% Co-op Head, Co-op Node or raw Pid.
relay_data(#coop{instances=Instances}, Data) ->
    case Instances of
        #coop_instance{} = One_Inst -> relay_data(One_Inst, Data);
        _Ets_Table                  -> not_implemented_yet
    end,
    ok;
relay_data(#coop_instance{head=none}, _Data)     -> ok;
relay_data(#coop_instance{head=Coop_Head}, Data) -> coop_head:send_data_msg(Coop_Head, Data), ok;
relay_data(#coop_head{} = Coop_Head, Data)       -> coop_head:send_data_msg(Coop_Head, Data), ok;
relay_data(#coop_node{} = Coop_Node, Data)       -> coop_node:node_task_deliver_data(Coop_Node, Data), ok;
relay_data(Pid, Data) when is_pid(Pid)           -> Pid ! Data, ok;
relay_data(none, _Data)                          -> ok.

%% High priority only works for a Coop_Instance or Coop_Head,
%% bypassing all pending Data requests.
relay_high_priority_data(#coop_instance{head=Coop_Head}, Data) ->
    coop_head:send_priority_data_msg(Coop_Head, Data),
    ok;
relay_high_priority_data(#coop_head{} = Coop_Head, Data) ->
    coop_head:send_priority_data_msg(Coop_Head, Data),
    ok;
relay_high_priority_data(Dest, Data) ->
    relay_data(Dest, Data),
    ok.


%%----------------------------------------------------------------------
%% Pipeline patterns
%%----------------------------------------------------------------------
pipeline(Coop_Instance, Kill_Switch, Left_To_Right_Stages, Receiver) ->
    Coops_Graph = digraph:new([acyclic]),
    digraph:add_vertex(Coops_Graph, outbound, Receiver),
    {First_Stage_Coop_Node, _Second_Stage_Vertex_Name} =
        lists:foldr(fun(Node_Name_Fn_Pair, {_NextStage, _Downstream_Vertex_Name} = Acc) ->
                            spawn_pipeline_stage(Coop_Instance, Kill_Switch, Coops_Graph, Node_Name_Fn_Pair, Acc)
                    end, {Receiver, outbound}, Left_To_Right_Stages),

    %% Return the coop_instance with the live coop_node graph set.
    set_coop_root_node(Coop_Instance, First_Stage_Coop_Node),
    Coop_Instance#coop_instance{body=First_Stage_Coop_Node, dag=Coops_Graph}.

spawn_pipeline_stage(Coop_Instance, Kill_Switch, Graph,
                     {Name, #coop_node_fn{init=Init_Fn, task=Task_Fn, options=Opts}},
                     {Receiver, Downstream_Vertex_Name}) ->
    Coop_Node = coop_node:new(Coop_Instance, Kill_Switch, Task_Fn, Init_Fn, Opts),  % Defaults to broadcast out
    coop_node:node_task_add_downstream_pids(Coop_Node, [Receiver]),             % And just 1 receiver
    digraph:add_vertex(Graph, Name, Coop_Node),
    digraph:add_edge(Graph, Name, Downstream_Vertex_Name),
    {Coop_Node, Name}.
    

%%----------------------------------------------------------------------
%% Fanout patterns
%%----------------------------------------------------------------------
fanout(Inbound, Coop_Instance, Kill_Switch, Fanout_Template_Graph) ->
    Coops_Graph = digraph:new([acyclic]),
    {Inbound, #coop_node_fn{init=Inbound_Init_Fn, task=Inbound_Task_Fn, options=Opts, flow=Inbound_Dataflow}}
        = digraph:vertex(Fanout_Template_Graph, Inbound),
    Inbound_Node = coop_node:new(Coop_Instance, Kill_Switch, Inbound_Task_Fn, Inbound_Init_Fn, Opts, Inbound_Dataflow),
    digraph:add_vertex(Coops_Graph, Inbound, Inbound_Node),
    {Has_Fan_In, Rcvr} = case digraph:vertex(Fanout_Template_Graph, outbound) of
                             false -> {false, none};
                             {outbound, Receiver} ->
                                 digraph:add_vertex(Coops_Graph, outbound, Receiver),
                                 {true, Receiver}
                         end,
    Worker_Nodes = [add_fanout_worker_node(Coop_Instance, Kill_Switch, Inbound, Has_Fan_In, Rcvr, Fanout_Template_Graph, Vertex_Name, Coops_Graph)
                    || Vertex_Name <- digraph:out_neighbours(Fanout_Template_Graph, Inbound)],
    coop_node:node_task_add_downstream_pids(Inbound_Node, Worker_Nodes),

    %% Return the coop_instance with the live coop_node graph set.
    set_coop_root_node(Coop_Instance, Inbound_Node),
    Coop_Instance#coop_instance{body=Inbound_Node, dag=Coops_Graph}.

add_fanout_worker_node(Coop_Instance, Kill_Switch, Inbound, Has_Fan_In, Receiver, Template_Graph, Vertex_Name, Coops_Graph) ->
    {Vertex_Name, #coop_node_fn{init=Init_Fn, task=Task_Fn, options=Opts}}
        = digraph:vertex(Template_Graph, Vertex_Name),
    Coop_Node = coop_node:new(Coop_Instance, Kill_Switch, Task_Fn, Init_Fn, Opts),  % Defaults to broadcast
    digraph:add_vertex(Coops_Graph, Vertex_Name, Coop_Node),
    digraph:add_edge(Coops_Graph, Inbound, Vertex_Name),
    Has_Fan_In andalso begin
                           digraph:add_edge(Coops_Graph, Vertex_Name, outbound),
                           coop_node:node_task_add_downstream_pids(Coop_Node, [Receiver])
                       end,
    Coop_Node.
    
