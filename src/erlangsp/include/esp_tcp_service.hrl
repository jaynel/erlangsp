-record(esp_tcp_service, {
          ip               :: string(),
          port             :: pos_integer(),
          socket           :: gen_tcp:socket(),
          client_module    :: module(),
          active = true    :: boolean(),
          connect_time = 0 :: non_neg_integer()
         }).
-type esp_tcp_service() :: #esp_tcp_service{}.
