
%% Coop command tokens are uppercase prefixed with '$$_'.
%% Applications should avoid using this prefix for atoms.
-define(DAG_TOKEN,  '$$_DAG').
-define(DATA_TOKEN, '$$_DATA').
-define(CTL_TOKEN,  '$$_CTL').
-define(ROOT_TOKEN, '$$_ROOT').

%% Coop data tokens are uppercase prefixed with '##_'.
%% Applications should avoid using this prefix for atoms.
-define(COOP_NOOP,  '##_NOOP').

-define(CTL_MSG(__Msg), {?DAG_TOKEN, ?CTL_TOKEN, __Msg}).
-define(CTL_MSG(__Msg, __Flag, __From),  {?DAG_TOKEN, ?CTL_TOKEN, __Msg, __Flag, __From}).
-define(DATA_MSG(__Msg), {?DAG_TOKEN, ?DATA_TOKEN, __Msg}).

-define(COOP_INIT_FN(__Fun, __Args), {?MODULE, __Fun, __Args}).
-define(COOP_TASK_FN(__Fun), {?MODULE, __Fun}).
