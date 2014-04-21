
%%----------------------------------------------------------------------
%% A Co-op Node is a single worker element of a Co-op. Every worker
%% element exists to accept data, transform it and pass it on.
%%----------------------------------------------------------------------

-record(coop_node_state, {
          kill_switch :: pid(),           % terminate the entire co-op
          ctl         :: pid(),           % receive control requests
          task        :: pid(),           % execute the task_fn
          init_fn     :: coop_init_fn(),  % init the task
          task_fn     :: coop_task_fn(),  % transform the data
          trace       :: pid(),           % relay trace information
          log         :: pid(),           % record log and telemetry data
          reflect     :: pid()            % reflect data flow for display
         }).
