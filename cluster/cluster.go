package cluster

var ServerRole string ="LEADER"
//either a LEADER or a FOLLOWER 
var LeaderAddress =""


//A list of File Descriptors (FDs) representing connected Replica nodes
//because my followers are Individual separated OS processes in this case 
var ReplicaFDs = make([]int,0)
