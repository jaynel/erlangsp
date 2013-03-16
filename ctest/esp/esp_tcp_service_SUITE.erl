-module(esp_tcp_service_SUITE).

-include("../../src/license_and_copyright.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("coop/include/coop.hrl").
-include_lib("coop/include/coop_dag.hrl").

%% Suite functions
-export([all/0, init_per_suite/1, end_per_suite/1]).

%% Test message flow control for a service
-export([start_and_stop/1]).
-export([init_coop/1, coop_query/2]).

all() -> [start_and_stop].

init_per_suite(Config) -> Config.
end_per_suite(_Config) -> ok.

-define(ES, esp_service).
-define(TS, esp_tcp_service).


%%----------------------------------------------------------------------
%% Test functions
%%----------------------------------------------------------------------
start_and_stop(_Config) ->
    Fake_Coop = make_coop(),
    Tcp_Service = ?TS:new_active(5, Fake_Coop),
    new = ?ES:status(Tcp_Service),
    {error, not_started} = ?ES:act_on(Tcp_Service, 5),
    Started_Service = ?TS:start(Tcp_Service, [{client_module, ?MODULE}, {port, 8200}]),
    active = ?ES:status(Started_Service),
    Stopped_Service = ?TS:stop(Started_Service),
    stopped = ?ES:status(Stopped_Service),
    {error, stopped} = ?ES:act_on(Stopped_Service, 4),
    ok.

make_coop() ->
    Init_Fn = ?COOP_INIT_FN(init_coop, {}),
    Task_Fn = ?COOP_TASK_FN(coop_query),
    Coop_Master = coop:make_dag_node(inbound, Init_Fn, Task_Fn, [], broadcast),
    coop:new_fanout(Coop_Master, [], none).

init_coop({}) -> {}.
coop_query({}, coop_query) -> coop_query.

    
