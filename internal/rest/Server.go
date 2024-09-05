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
	http.HandleFunc("/range",s.service.HandleRange)
	http.HandleFunc("/batch", s.service.HandleBatch)
	http.HandleFunc("/scankey",s.service.HandleScanKey)
	http.HandleFunc("/scanvaluebykey",s.service.HandleScanValueByKey)
	http.HandleFunc("/scankeylower",s.service.HandleScanKeysLower)

	http.HandleFunc("/scanoffset",s.service.HandleScanOffset)
	http.HandleFunc("/totalkey",s.service.HandleTotalKey)

}


func (s *Server) Start(addr string) error {
	s.SetupRoutes()
	return http.ListenAndServe(addr, nil)
}
