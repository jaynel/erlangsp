%% -*- mode: erlang -*-

{include, ["../include"]}.
{cover,   "./erlangsp.coverspec"}.
{alias,   erlangsp, "./erlangsp/"}.
{suites,  erlangsp, [
                     erlangsp_SUITE
                    ]}.
