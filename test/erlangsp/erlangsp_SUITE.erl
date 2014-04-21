%%%------------------------------------------------------------------------------
%%% @copyright (c) 2014, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com> [http://duomark.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   Common Test for erlangsp app and supervisor.
%%% @since v0.1.0
%%% @end
%%%------------------------------------------------------------------------------
-module(erlangsp_SUITE).
-author('jay@duomark.com').
-vsn('').

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([
         check_app/1,
         check_service/1
        ]).

-include("../erlangsp_common_test.hrl").
-include("coop.hrl").
-include("esp_service.hrl").

all() -> [
          check_app,
          check_service
         ].

init_per_suite(Config) -> Config.
end_per_suite(Config)  -> Config.

-spec check_app(config()) -> ok.
check_app(_Config) ->

    ct:log("Start the erlangsp application"),
    ok = application:start(erlangsp),
    Started_Apps = application:which_applications(),
    [{erlangsp,"Erlang Services Platform Library","0.1.0"}]
        = [App || App = {erlangsp, _Desc, _Vsn} <- Started_Apps],
    {ok, erlangsp} = application:get_application(erlangsp_app),
    {ok, erlangsp} = application:get_application(erlangsp_sup),

    ct:log("Verify the root supervisor is running"),
    Sup_Pid = whereis(erlangsp_sup),
    [] = supervisor:which_children(Sup_Pid),
    ok = application:stop(erlangsp),
    After_Stop_Apps = application:which_applications(),
    [] = [App || App = {erlangsp, _Desc, _Vsn} <- After_Stop_Apps],
    undefined = whereis(erlangsp_sup),
    ok.

-spec check_service(config()) -> ok.
check_service(_Config) ->

    ct:log("Start a new esp_service"),
    Firehose = #coop_instance{id=1, name=firehose},
    Coop = #coop{instances=Firehose, dataflow=round_robin},
    #esp_svc{coop=Coop} = esp_service:make_service(Coop),
    ok.
