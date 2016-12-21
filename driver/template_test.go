package driver

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
	"testing"
)

func TestTemplateParses(t *testing.T) {
	assert := require.New(t)

	volConf := &volumeConfig{
		Name:             "foo",
		SizeGB:           "11GB",
		ReadIOPS:         "11000",
		WriteIOPS:        "10000",
		ReplicaBaseImage: "rancher/vm-ubuntu",
	}
	dockerCompose := new(bytes.Buffer)
	if err := composeTemplate.Execute(dockerCompose, volConf); err != nil {
		t.Fatalf("Error while executing template %v", err)
	}
	fmt.Printf("%s", dockerCompose)

	fmt.Println("\n\n-----------\b\b")
	volConf.ReplicaBaseImage = ""
	dockerCompose = new(bytes.Buffer)
	if err := composeTemplate.Execute(dockerCompose, volConf); err != nil {
		t.Fatalf("Error while executing template %v", err)
	}
	fmt.Printf("%s", dockerCompose)

	d := new(map[interface{}]interface{})
	err := yaml.Unmarshal(dockerCompose.Bytes(), d)
	assert.Nil(err)
	assert.Equal(
		"/var/lib/rancher/longhorn/backups:/var/lib/rancher/longhorn/backups",
		(*d)["replica"].(map[interface{}]interface{})["volumes"].([]interface{})[1],
	)
}
