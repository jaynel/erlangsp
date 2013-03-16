%%%------------------------------------------------------------------------------
%%% @copyright (c) 2012, DuoMark International, Inc.  All rights reserved
%%% @author Jay Nelson <jay@duomark.com>
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%    Receive loop for the Coop Kill Switch process.
%%% @since v0.0.1
%%% @end
%%%------------------------------------------------------------------------------
-module(coop_kill_link_rcv).

-include("../../license_and_copyright.hrl").
-author('Jay Nelson <jay@duomark.com>').

%% Graph API
-export([make_kill_switch/0, link_loop/0,
         link_to_kill_switch/2, unlink_from_kill_switch/2]).

-include("coop_dag.hrl").

-spec make_kill_switch() -> pid().
make_kill_switch() -> proc_lib:spawn(?MODULE, link_loop, []).

link_to_kill_switch(Kill_Switch, Procs) when is_list(Procs) ->
    Kill_Switch ! ?CTL_MSG({link, Procs}).

unlink_from_kill_switch(Kill_Switch, Procs) when is_list(Procs) ->
    Kill_Switch ! ?CTL_MSG({unlink, Procs}).

link_loop() ->
    receive

        %%------------------------------------------------------------
        %% TODO: This code needs to be improved to handle remote
        %% support processes. Right now all are assumed to be local
        %% to the coop_head and coop_node's erlang VM node.
        ?CTL_MSG({link, Procs}) ->
            [case is_process_alive(P) of

                 %% Crash if process to link is already dead
                 false -> [exit(Pid, kill) || Pid <- Procs], exit(kill);

                 %% Otherwise link and continue
                 true  -> link(P)

             end || P <- Procs],
            link_loop();

        ?CTL_MSG({unlink, Procs}) ->
            [unlink(P) || P <- Procs],
            link_loop();
        %%------------------------------------------------------------

        _Unknown ->
            error_logger:error_msg("~p ~p: Ignoring ~p~n", [?MODULE, self(), _Unknown]),
            link_loop()
    end.
