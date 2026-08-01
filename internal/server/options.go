package server

// Server encapsulates the state for the Epoll and Raft server
type Server struct {
	port       int
	nodeId     int32
	raftPort   int
	peerIPs    []string
	maxClients int
	epollFD    int
	serverFD   int
}

type Option func(*Server)

func WithPort(port int) Option {
	return func(s *Server) { s.port = port }
}

func WithNodeId(id int32) Option {
	return func(s *Server) { s.nodeId = id }
}

func WithRaftPort(port int) Option {
	return func(s *Server) { s.raftPort = port }
}

func WithPeerIPs(ips []string) Option {
	return func(s *Server) { s.peerIPs = ips }
}

func WithMaxClients(max int) Option {
	return func(s *Server) { s.maxClients = max }
}

// New creates a Server
func New(opts ...Option) *Server {
	s := &Server{
		port:       6379,
		maxClients: 10000,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}
