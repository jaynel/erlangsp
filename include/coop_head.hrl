
%%----------------------------------------------------------------------
%% A Coop Head is the external interface of a coop graph.
%% It receives control and data requests and passes them on to
%% the Coop Node components of the coop graph.
%%----------------------------------------------------------------------

-record(coop_head_state, {
          kill_switch :: pid(),  % terminate the entire coop
          ctl         :: pid(),  % receive control requests
          data        :: pid(),  % forward data requests
          root        :: pid(),  % one_at_a_time gateway to Coop Body
          log         :: pid(),  % record log and telemetry data
          trace       :: pid(),  % relay trace information
          reflect     :: pid(),  % reflect data flow for display/analysis
          coop_root_node = none :: coop_body() | none
         }).
