-ifdef(sentinerl).
-define(begin_checkpoint(Id, Checkpoint), sentinerl:begin(Id, Checkpoint)).
-define(set_checkpoint(Id, Checkpoint), sentinerl:begin(Id, Checkpoint)).
-define(end_checkpoint(Id, Checkpoint), sentinerl:begin(Id, Checkpoint)).

-else.
-define(begin_checkpoint(Id, Checkpoint), true).
-define(set_checkpoint(Id, Checkpoint), true).
-define(end_checkpoint(Id, Checkpoint), true).

-endif.

