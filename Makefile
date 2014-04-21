PROJECT   = erlangsp
ERL_LIBS  = /Users/jay/Git/proper

CT_OPTS        = -cover test/erlangsp.coverspec -logdir test/logs
CT_SUITES      = erlangsp

include erlang.mk
