package raft



import(
	"log"
	

)

var (
	RaftPort=10000
	NodeId= int32(1)
)

var peerIPs []string = []string{"127.0.0.1:10001", "127.0.0.1:10002"}

func main(){
    raftBrain := New(NodeId, peerIPs)

	go func(){
		err:= StartrpcServer(RaftPort,raftBrain)
		if err!=nil{
			log.Fatalf("Fatal :Rfat grpc erver crashed abruptly",err)

		}
	}()

 // Blocking forever just for this boilerplate test
	select {} 

}