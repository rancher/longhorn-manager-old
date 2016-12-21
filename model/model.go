package model

type Volume struct {
	Name       string            `json:"name"`
	Mountpoint string            `json:"mountpoint"`
	Opts       map[string]string `json:"opts"`
}
