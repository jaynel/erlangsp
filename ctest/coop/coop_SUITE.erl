-module(coop_SUITE).

-include("../../src/license_and_copyright.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("coop/include/coop.hrl").
-include_lib("coop/include/coop_dag.hrl").

%% Suite functions
-export([all/0, init_per_suite/1, end_per_suite/1]).

%% Pipeline and fanout tests
-export([pipeline_flow/1, pipeline_failure/1, pipeline/1,
         fanout_flow/1, fanout_failure/1,
         fanout_round_robin/1, fanout_broadcast/1,
         fanout_migrate/1
        ]).

%% Node task and init functions
-export([init/1, plus2/2, times3/2, minus5/2, rr_init/1, rr_inc/2]).

%% Test procs for validating process message output
-export([receive_pipe_results/0, receive_round_robin_results/3]).
 
all() -> [pipeline_flow, pipeline_failure, pipeline, 
          fanout_flow, fanout_failure,
          fanout_round_robin, fanout_broadcast,
          fanout_migrate
         ].

init_per_suite(Config) -> Config.
end_per_suite(_Config) -> ok.


%%----------------------------------------------------------------------
%% Pipeline patterns
%%----------------------------------------------------------------------
pipeline_failure(_Config) ->
    try coop_flow:pipeline(a)
    catch error:function_clause -> ok
    end,

    try coop_flow:pipeline([a])
    catch error:function_clause -> ok
    end.

init([f1]) -> f1;
init([f2]) -> f2;
init([f3]) -> f3.

%% Init state and looping state are unused, but checked placeholders.
plus2(f1, Num)  -> {f1, Num+2}.
times3(f2, Num) -> {f2, Num*3}.
minus5(f3, Num) -> {f3, Num-5}.
    
example_pipeline_fns() ->
    %% Pipeline => 3 * (X+2) - 5
    F1_Init = {?MODULE, init, [f1]},
    F2_Init = {?MODULE, init, [f2]},
    F3_Init = {?MODULE, init, [f3]},

    F1_Task = {?MODULE, plus2},
    F2_Task = {?MODULE, times3},
    F3_Task = {?MODULE, minus5},

    F1_Node_Fn = #coop_node_fn{init=F1_Init, task=F1_Task},
    F2_Node_Fn = #coop_node_fn{init=F2_Init, task=F2_Task},
    F3_Node_Fn = #coop_node_fn{init=F3_Init, task=F3_Task},

    [
     #coop_dag_node{name=a, label=F1_Node_Fn},
     #coop_dag_node{name=b, label=F2_Node_Fn},
     #coop_dag_node{name=c, label=F3_Node_Fn}
    ].

pipeline_flow(_Config) ->
    Pipe_Stages = example_pipeline_fns(),
    Pipeline = coop_flow:pipeline(Pipe_Stages),
    Pipe_Stats = digraph:info(Pipeline),
    acyclic = proplists:get_value(cyclicity, Pipe_Stats),

    %% Check a -> b -> c...
    3 = digraph:no_vertices(Pipeline),
    2 = digraph:no_edges(Pipeline),

    %% Unidirectional flow...
    [a,b,c] = digraph:get_path(Pipeline, a, c),
    false   = digraph:get_path(Pipeline, c, a),
    
    %% Check graph vertices.
    3 = length(digraph:vertices(Pipeline)),
    [A, B, C] = [{N, L} || #coop_dag_node{name=N, label=L} <- Pipe_Stages],
    A = digraph:vertex(Pipeline, a),
    B = digraph:vertex(Pipeline, b),
    C = digraph:vertex(Pipeline, c).

pipeline(_Config) ->
    Pid = spawn_link(?MODULE, receive_pipe_results, []),
    Coop = coop:new_pipeline(example_pipeline_fns(), Pid),
    Coops_Graph = (Coop#coop.instances)#coop_instance.dag,
    Pipe_Stats = digraph:info(Coops_Graph),
    acyclic = proplists:get_value(cyclicity, Pipe_Stats),
    coop:relay_data(Coop, 7),
    timer:sleep(100),
    ok = fetch_results(Pid).
    

receive_pipe_results() ->
    receive
        3 * (7+2) - 5 -> hold_results(ok);
        Other ->  hold_results({fail, Other})
    after 3000 -> hold_results(timeout)
    end.


%%----------------------------------------------------------------------
%% Fanout patterns
%%----------------------------------------------------------------------
fanout_failure(_Config) ->
    try coop_flow:fanout(a, 8, self())
    catch error:function_clause -> ok
    end,
    
    try coop_flow:fanout(#coop_dag_node{}, a, self())
    catch error:function_clause -> ok
    end.

check_fanout_vertex(Graph, #coop_dag_node{label=Label}, Inbound, InDegree, OutDegree) ->
    {Inbound, Label} = digraph:vertex(Graph, Inbound),
    InDegree   = digraph:in_degree(Graph, Inbound),
    OutDegree  = digraph:out_degree(Graph, Inbound),
    InDegree   = length(digraph:in_neighbours(Graph, Inbound)),
    OutDegree  = length([V || V <- digraph:out_neighbours(Graph, Inbound)]);
check_fanout_vertex(Graph, Pid, outbound = Outbound, InDegree, OutDegree) ->
    {Outbound, Pid} = digraph:vertex(Graph, Outbound),
    InDegree    = digraph:in_degree(Graph, Outbound),
    OutDegree   = digraph:out_degree(Graph, Outbound),
    InDegree    = length([V || V <- digraph:in_neighbours(Graph, Outbound)]),
    OutDegree   = length(digraph:out_neighbours(Graph, Outbound));
check_fanout_vertex(Graph, _N, {Name, Inbound, _Fn}, 1, 1) ->    
    {Name, #coop_node_fn{}} = digraph:vertex(Graph, Name),
    [Inbound] =  digraph:in_neighbours(Graph, Name),
    [outbound] = digraph:out_neighbours(Graph, Name).

fanout_flow(_Config) ->
    Self = self(),
    Router_Fn = #coop_dag_node{
      name = funnel,
      label = #coop_node_fn{init={?MODULE, init, [f2]}, task={?MODULE, times3}}
     },
    Worker_Node_Fns = [#coop_dag_node{
                          name = N,
                          label = #coop_node_fn{init={?MODULE, init, [f3]}, task={?MODULE, minus5}}}
                       || N <- lists:seq(1,8)],
    Coop_Flow = coop_flow:fanout(Router_Fn, Worker_Node_Fns, Self),
    10 = digraph:no_vertices(Coop_Flow),
    16 = digraph:no_edges(Coop_Flow),
    check_fanout_vertex(Coop_Flow, Router_Fn, funnel,  0, 8),
    check_fanout_vertex(Coop_Flow, Self, outbound, 8, 0),
    [check_fanout_vertex(Coop_Flow, 8, {N,funnel,#coop_node_fn{}}, 1, 1) || N <- lists:seq(1,8)].

make_fanout_coop(Dataflow_Type, Num_Workers, Receiver_Pid) ->
    Router_Fn = #coop_dag_node{
      name = inbound,
      label = #coop_node_fn{init={?MODULE, rr_init, [0]}, task={?MODULE, rr_inc}, flow=Dataflow_Type}
     },
    Worker_Node_Fns = [#coop_dag_node{
                          name = "inc_by_" ++ integer_to_list(N),
                          label = #coop_node_fn{init={?MODULE, rr_init, [N]}, task={?MODULE, rr_inc}}}
                       || N <- lists:seq(1, Num_Workers)],
    coop:new_fanout(Router_Fn, Worker_Node_Fns, Receiver_Pid).
    
fanout_round_robin(_Config) ->
    Num_Results = 6,
    Num_Workers = 3,
    Receiver_Pid = spawn_link(?MODULE, receive_round_robin_results, [Num_Results, [], Num_Results]),
    Coop = make_fanout_coop(round_robin, Num_Workers, Receiver_Pid),
    Coops_Graph = (Coop#coop.instances)#coop_instance.dag,
    Fanout_Stats = digraph:info(Coops_Graph),
    acyclic = proplists:get_value(cyclicity, Fanout_Stats),
    5 = digraph:no_vertices(Coops_Graph),
    6 = digraph:no_edges(Coops_Graph),
    [coop:relay_data(Coop, 5) || _N <- lists:seq(1, Num_Results)],
    timer:sleep(100),
    Results6 = fetch_results(Receiver_Pid),
    6 = length(Results6),
    Results4 = Results6 -- [6,6],
    4 = length(Results4),
    Results2 = Results4 -- [7,7],
    2 = length(Results2),
    Results0 = Results2 -- [8,8],
    0 = length(Results0).

%% State is an amount to increment; dataflow returns constant State and incremented input.
rr_init([Inc_Amt]) -> Inc_Amt.
rr_inc(Inc_Amt, Value) -> {Inc_Amt, Value + Inc_Amt}.
    
receive_round_robin_results(0, Acc, N) ->
    hold_results(lists:reverse(Acc)),
    receive_round_robin_results(N, [], N);
receive_round_robin_results(M, Acc, N) ->
    receive Any -> receive_round_robin_results(M-1, [Any | Acc], N)
    after  3000 -> hold_results([timeout | Acc])
    end.
    
fanout_broadcast(_Config) ->
    Num_Results = 12,
    Num_Workers = 4,
    Receiver_Pid = spawn_link(?MODULE, receive_round_robin_results, [Num_Results, [], Num_Results]),
    Coop = make_fanout_coop(broadcast, Num_Workers, Receiver_Pid),
    Coops_Graph = (Coop#coop.instances)#coop_instance.dag,
    Fanout_Stats = digraph:info(Coops_Graph),
    acyclic = proplists:get_value(cyclicity, Fanout_Stats),
    6 = digraph:no_vertices(Coops_Graph),
    8 = digraph:no_edges(Coops_Graph),
    [coop:relay_data(Coop, 7) || _N <- lists:seq(1, Num_Results div Num_Workers)],
    timer:sleep(100),
    Results12 = fetch_results(Receiver_Pid),
    12 = length(Results12),
    Results9 = Results12 -- [8,8,8],
    9 = length(Results9),
    Results6 = Results9 -- [9,9,9],
    6 = length(Results6),
    Results3 = Results6 -- [10,10,10],
    3 = length(Results3),
    Results0 = Results3 -- [11,11,11],
    0 = length(Results0).

fanout_migrate(_Config) ->
    Num_Results = 2,
    Num_Workers = 4,
    Receiver_Pid = spawn_link(?MODULE, receive_round_robin_results, [Num_Results, [], Num_Results]),
    Coop1 = make_fanout_coop(broadcast, Num_Workers, Receiver_Pid),
    Coops_Graph = (Coop1#coop.instances)#coop_instance.dag,
    Fanout_Stats = digraph:info(Coops_Graph),
    acyclic = proplists:get_value(cyclicity, Fanout_Stats),
    6 = digraph:no_vertices(Coops_Graph),
    8 = digraph:no_edges(Coops_Graph),
    coop:relay_data(Coop1, 9),
    timer:sleep(100),
    Results1 = fetch_results(Receiver_Pid),
    2 = length(Results1),
    Results2 = fetch_results(Receiver_Pid),
    2 = length(Results2),
    [] = ([10, 11, 12, 13] -- Results1) -- Results2,
    timeout_fetching = fetch_results(Receiver_Pid),
    Head1 = coop:head(Coop1),
    Root_Node1 = coop_head:get_root_node(Head1),
    [Node1, Node2, Node3, Node4] = coop_node:node_task_get_downstream_pids(Root_Node1),
    Coop2 = make_fanout_coop(broadcast, 0, none),
    Head2 = coop:head(Coop2),
    Root_Node2 = coop_head:get_root_node(Head2),
    ct:log("Node1: ~p~nInstance1: ~p~nInstance2: ~p~n",
           [Node1, Coop1#coop.instances, Coop2#coop.instances]),
    ok = coop:migrate_fanout_node(Node1, Coop1#coop.instances, Coop2#coop.instances),
    [Node2, Node3, Node4] = coop_node:node_task_get_downstream_pids(Root_Node1),
    [Node1] = coop_node:node_task_get_downstream_pids(Root_Node2).
    

%%----------------------------------------------------------------------
%% Utilities for receiving coop results
%%----------------------------------------------------------------------
fetch_results(Pid) ->
    Pid ! {fetch, self()},
    receive Any -> Any
    after 3000 -> timeout_fetching
    end.

hold_results(Results) ->
    receive
        {fetch, From} -> From ! Results
    after 3000 -> timeout
    end.
