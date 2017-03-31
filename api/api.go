package api

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"
)

const Port int = 7000

func HandleError(s *client.Schemas, t func(http.ResponseWriter, *http.Request) error) http.Handler {
	return api.ApiHandler(s, http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		if err := t(rw, req); err != nil {
			apiContext := api.GetApiContext(req)
			apiContext.WriteErr(err)
		}
	}))
}

func Handler(s *Server) http.Handler {
	r := mux.NewRouter().StrictSlash(true)
	schemas := NewSchema()
	f := HandleError
	fo := api.ApiHandler // To be replaced later

	versionsHandler := api.VersionsHandler(schemas, "v1")
	versionHandler := api.VersionHandler(schemas, "v1")
	r.Methods("GET").Path("/").Handler(versionsHandler)
	r.Methods("GET").Path("/v1").Handler(versionHandler)
	r.Methods("GET").Path("/v1/apiversions").Handler(versionsHandler)
	r.Methods("GET").Path("/v1/apiversions/v1").Handler(versionHandler)
	r.Methods("GET").Path("/v1/schemas").Handler(api.SchemasHandler(schemas))
	r.Methods("GET").Path("/v1/schemas/{id}").Handler(api.SchemaHandler(schemas))

	r.Methods("GET").Path("/v1/settings").Handler(f(schemas, s.settings.Get))
	r.Methods("PUT").Path("/v1/settings").Handler(f(schemas, s.settings.Set))

	r.Methods("GET").Path("/v1/volumes").Handler(f(schemas, s.ListVolume))
	r.Methods("GET").Path("/v1/volumes/{name}").Handler(f(schemas, s.GetVolume))
	r.Methods("DELETE").Path("/v1/volumes/{name}").Handler(f(schemas, s.DeleteVolume))
	r.Methods("POST").Path("/v1/volumes").Handler(f(schemas, s.CreateVolume))

	r.Methods("GET").Path("/v1/backups").Queries("volume", "{volName}").
		Handler(fo(schemas, http.HandlerFunc(s.backups.List)))
	r.Methods("GET").Path("/v1/backups/{backupName}").Queries("volume", "{volName}").
		Handler(fo(schemas, http.HandlerFunc(s.backups.Get)))
	r.Methods("DELETE").Path("/v1/backups/{backupName}").Queries("volume", "{volName}").
		Handler(fo(schemas, http.HandlerFunc(s.backups.Delete)))

	r.Methods("POST").Path("/v1/volumes/{name}/attach").
		Handler(fo(schemas, s.fwd.Handler(HostIDFromAttachReq, NameFunc(s.man.Attach))))
	r.Methods("POST").Path("/v1/volumes/{name}/detach").
		Handler(fo(schemas, s.fwd.Handler(HostIDFromVolume(s.man), NameFunc(s.man.Detach))))

	r.Methods("POST").Path("/v1/volumes/{name}/snapshots").
		Handler(fo(schemas, s.fwd.Handler(HostIDFromVolume(s.man), s.snapshots.Create)))
	r.Methods("GET").Path("/v1/volumes/{name}/snapshots").
		Handler(fo(schemas, s.fwd.Handler(HostIDFromVolume(s.man), s.snapshots.List)))
	r.Methods("GET").Path("/v1/volumes/{name}/snapshots/{snapName}").
		Handler(fo(schemas, s.fwd.Handler(HostIDFromVolume(s.man), s.snapshots.Get)))
	r.Methods("DELETE").Path("/v1/volumes/{name}/snapshots/{snapName}").
		Handler(fo(schemas, s.fwd.Handler(HostIDFromVolume(s.man), s.snapshots.Delete)))
	r.Methods("POST").Path("/v1/volumes/{name}/snapshots/{snapName}/revert").
		Handler(fo(schemas, s.fwd.Handler(HostIDFromVolume(s.man), s.snapshots.Revert)))
	r.Methods("POST").Path("/v1/volumes/{name}/snapshots/{snapName}/backup").
		Handler(fo(schemas, s.fwd.Handler(HostIDFromVolume(s.man), s.snapshots.Backup)))

	r.Methods("GET").Path("/v1/hosts").Handler(f(schemas, s.ListHost))
	r.Methods("GET").Path("/v1/hosts/{id}").Handler(f(schemas, s.GetHost))

	return r
}
