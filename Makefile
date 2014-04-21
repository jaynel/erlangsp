PROJECT   = erlangsp
ERL_LIBS  = /Users/jay/Git/proper

CT_OPTS   = -logdir test/logs -cover test/erlangsp.coverspec
CT_SUITES = erlangsp

include erlang.mk
