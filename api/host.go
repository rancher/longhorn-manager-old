package api

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"
)

func (s *Server) ListHost(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	hosts, err := s.man.ListHosts()
	if err != nil {
		return errors.Wrap(err, "fail to list host")
	}
	apiContext.Write(toHostCollection(hosts))
	return nil
}

func (s *Server) GetHost(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)
	id := mux.Vars(req)["id"]

	host, err := s.man.GetHost(id)
	if err != nil {
		return errors.Wrap(err, "fail to get host")
	}
	apiContext.Write(toHostResource(host))
	return nil
}
