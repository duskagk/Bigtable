package rest

import "net/http"


type Server struct {
	service RESTService
}

func NewServer(service RESTService) *Server {
	return &Server{
		service: service,
	}
}

func (s *Server) SetupRoutes() {
	http.HandleFunc("/set", s.service.HandleSet)
	http.HandleFunc("/get", s.service.HandleGet)
	http.HandleFunc("/delete", s.service.HandleDelete)
	http.HandleFunc("/createTable", s.service.HandleCreateTable)
}

func (s *Server) Start(addr string) error {
	s.SetupRoutes()
	return http.ListenAndServe(addr, nil)
}

