# actor

A distributed system base on actor model,uses zookeeper for synchronousing information of distributed system.
actor is base programming unit of the framework,
each actor has a unique path registed in zookeeper,and a mailbox for receiving message from other actors,
use Context().Tell(path interface{},msg interface{}) to send msg to actor of path.
message pass between actors should register two func encoder and decoder in crntext.
dispatcher is responsible for distributing message for actors.

in pingpong_test,you can see 2 balls are passed between 4 players.