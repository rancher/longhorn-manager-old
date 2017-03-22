package api

import (
	"github.com/gorilla/mux"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/longhorn-orc/types"
	"net/http"
)

const Port int = 7000

func Handler(man types.VolumeManager, sl types.ServiceLocator, proxy http.Handler) http.Handler {
	r := mux.NewRouter().StrictSlash(true)
	schemas := NewSchema()
	f := api.ApiHandler
	fwd := &Fwd{sl, proxy}
	snapshots := SnapshotHandlers{man}
	settings := SettingsHandlers{man.Settings()}
	backups := BackupsHandlers{man}

	versionsHandler := api.VersionsHandler(schemas, "v1")
	versionHandler := api.VersionHandler(schemas, "v1")
	r.Methods("GET").Path("/").Handler(versionsHandler)
	r.Methods("GET").Path("/v1").Handler(versionHandler)
	r.Methods("GET").Path("/v1/apiversions").Handler(versionsHandler)
	r.Methods("GET").Path("/v1/apiversions/v1").Handler(versionHandler)
	r.Methods("GET").Path("/v1/schemas").Handler(api.SchemasHandler(schemas))
	r.Methods("GET").Path("/v1/schemas/{id}").Handler(api.SchemaHandler(schemas))

	r.Methods("GET").Path("/v1/settings").Handler(f(schemas, http.HandlerFunc(settings.Get)))
	r.Methods("PUT").Path("/v1/settings").Handler(f(schemas, http.HandlerFunc(settings.Set)))

	r.Methods("GET").Path("/v1/volumes/").Handler(f(schemas, VolumeListFunc(man.List)))
	r.Methods("GET").Path("/v1/volumes/{name}").Handler(f(schemas, Name2VolumeFunc(man.Get)))
	r.Methods("DELETE").Path("/v1/volumes/{name}").Handler(f(schemas, NameFunc(man.Delete)))
	r.Methods("POST").Path("/v1/volumes/").Handler(f(schemas, Volume2VolumeFunc(man.Create)))

	r.Methods("GET").Path("/v1/backups/").Queries("volume", "{volName}").
		Handler(f(schemas, http.HandlerFunc(backups.List)))
	r.Methods("GET").Path("/v1/backups/{backupName}").Queries("volume", "{volName}").
		Handler(f(schemas, http.HandlerFunc(backups.Get)))
	r.Methods("DELETE").Path("/v1/backups/{backupName}").Queries("volume", "{volName}").
		Handler(f(schemas, http.HandlerFunc(backups.Delete)))

	r.Methods("POST").Path("/v1/volumes/{name}/attach").
		Handler(f(schemas, fwd.Handler(HostIDFromAttachReq, NameFunc(man.Attach))))
	r.Methods("POST").Path("/v1/volumes/{name}/detach").
		Handler(f(schemas, fwd.Handler(HostIDFromVolume(man), NameFunc(man.Detach))))

	r.Methods("POST").Path("/v1/volumes/{name}/snapshots/").
		Handler(f(schemas, fwd.Handler(HostIDFromVolume(man), snapshots.Create)))
	r.Methods("GET").Path("/v1/volumes/{name}/snapshots/").
		Handler(f(schemas, fwd.Handler(HostIDFromVolume(man), snapshots.List)))
	r.Methods("GET").Path("/v1/volumes/{name}/snapshots/{snapName}").
		Handler(f(schemas, fwd.Handler(HostIDFromVolume(man), snapshots.Get)))
	r.Methods("DELETE").Path("/v1/volumes/{name}/snapshots/{snapName}").
		Handler(f(schemas, fwd.Handler(HostIDFromVolume(man), snapshots.Delete)))
	r.Methods("POST").Path("/v1/volumes/{name}/snapshots/{snapName}/revert").
		Handler(f(schemas, fwd.Handler(HostIDFromVolume(man), snapshots.Revert)))
	r.Methods("POST").Path("/v1/volumes/{name}/snapshots/{snapName}/backup").
		Handler(f(schemas, fwd.Handler(HostIDFromVolume(man), snapshots.Backup)))

	return r
}
