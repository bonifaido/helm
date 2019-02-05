/*
Copyright The Helm Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package driver // import "k8s.io/helm/pkg/storage/driver"

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	kblabels "k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/validation"

	rspb "k8s.io/helm/pkg/proto/hapi/release"
	storageerrors "k8s.io/helm/pkg/storage/errors"
)

var _ Driver = (*Files)(nil)

// FilesDriverName is the string name of the driver.
const FilesDriverName = "Files"

// Files is a wrapper around an implementation of a kubernetes
// ConfigMapsInterface.
type Files struct {
	dir string
	Log func(string, ...interface{})
}

// NewFiles initializes a new ConfigMaps wrapping an implementation of
// the kubernetes ConfigMapsInterface.
func NewFiles(dir string) *Files {
	os.MkdirAll(dir, 0700)
	return &Files{
		dir: dir,
		Log: func(_ string, _ ...interface{}) {},
	}
}

// File holds all information about a release
type File struct {
	Labels kblabels.Set `json:"labels"`
	Data   string       `json:"data"`
}

func (f *File) Bytes() []byte {
	bytes, _ := json.Marshal(f)
	return bytes
}

func (f *File) Parse(data []byte) error {
	return json.Unmarshal(data, f)
}

// Name returns the name of the driver.
func (files *Files) Name() string {
	return FilesDriverName
}

// Get fetches the release named by key. The corresponding release is returned
// or error if not found.
func (files *Files) Get(key string) (*rspb.Release, error) {
	// fetch the file holding the release named by key
	data, err := ioutil.ReadFile(files.dir + "/" + key)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, storageerrors.ErrReleaseNotFound(key)
		}

		files.Log("get: failed to get %q: %s", key, err)
		return nil, err
	}

	file := &File{}
	err = file.Parse(data)
	if err != nil {
		files.Log("get: failed to parse release: %s", err)
		return nil, err
	}

	// found the configmap, decode the base64 data string
	r, err := decodeRelease(file.Data)
	if err != nil {
		files.Log("get: failed to decode data %q: %s", key, err)
		return nil, err
	}
	// return the release object
	return r, nil
}

// List fetches all releases and returns the list releases such
// that filter(release) == true. An error is returned if the
// configmap fails to retrieve the releases.
func (files *Files) List(filter func(*rspb.Release) bool) ([]*rspb.Release, error) {

	list, err := ioutil.ReadDir(files.dir)
	if err != nil {
		files.Log("list: failed to list: %s", err)
		return nil, err
	}

	var results []*rspb.Release

	// iterate over the configmaps object list
	// and decode each release
	for _, file := range list {
		if !file.IsDir() {
			rls, err := files.Get(file.Name())
			if err != nil {
				files.Log("list: failed to decode release: %v: %s", file, err)
				continue
			}
			if filter(rls) {
				results = append(results, rls)
			}
		}
	}
	return results, nil
}

// Query fetches all releases that match the provided map of labels.
// An error is returned if the configmap fails to retrieve the releases.
func (files *Files) Query(labels map[string]string) ([]*rspb.Release, error) {
	ls := kblabels.Set{}
	for k, v := range labels {
		if errs := validation.IsValidLabelValue(v); len(errs) != 0 {
			return nil, fmt.Errorf("invalid label value: %q: %s", v, strings.Join(errs, "; "))
		}
		ls[k] = v
	}

	selector := ls.AsSelector()

	list, err := ioutil.ReadDir(files.dir)
	if err != nil {
		files.Log("query: failed to list: %s", err)
		return nil, err
	}

	var results []*rspb.Release
	for _, item := range list {
		if !item.IsDir() {
			data, err := ioutil.ReadFile(files.dir + "/" + item.Name())
			if err != nil {
				files.Log("query: failed to read release: %s", err)
				continue
			}
			file := &File{}
			err = file.Parse(data)
			if err != nil {
				files.Log("query: failed to parse release: %s", err)
				continue
			}

			if selector.Matches(file.Labels) {
				rls, err := decodeRelease(file.Data)
				if err != nil {
					files.Log("query: failed to decode release: %s", err)
					continue
				}
				results = append(results, rls)
			}
		}
	}

	if len(results) == 0 {
		return nil, storageerrors.ErrReleaseNotFound(labels["NAME"])
	}

	return results, nil
}

// Create creates a new ConfigMap holding the release. If the
// ConfigMap already exists, ErrReleaseExists is returned.
func (files *Files) Create(key string, rls *rspb.Release) error {
	// set labels for configmaps object meta data
	var lbs labels

	lbs.init()
	lbs.set("CREATED_AT", strconv.Itoa(int(time.Now().Unix())))

	// create a new file to hold the release
	file, err := newFile(key, rls, lbs)
	if err != nil {
		files.Log("create: failed to encode release %q: %s", rls.Name, err)
		return err
	}
	// push the configmap object out into the disk
	if err := ioutil.WriteFile(files.dir+"/"+key, file.Bytes(), 0600); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return storageerrors.ErrReleaseExists(key)
		}

		files.Log("create: failed to create: %s", err)
		return err
	}
	return nil
}

// Update updates the ConfigMap holding the release. If not found
// the ConfigMap is created to hold the release.
func (files *Files) Update(key string, rls *rspb.Release) error {
	// set labels for configmaps object meta data
	var lbs labels

	lbs.init()
	lbs.set("MODIFIED_AT", strconv.Itoa(int(time.Now().Unix())))

	// create a new configmap object to hold the release
	file, err := newFile(key, rls, lbs)
	if err != nil {
		files.Log("update: failed to encode release %q: %s", rls.Name, err)
		return err
	}
	// push the configmap object out into the disk
	err = ioutil.WriteFile(files.dir+"/"+key, file.Bytes(), 0600)
	if err != nil {
		files.Log("update: failed to update: %s", err)
		return err
	}
	return nil
}

// Delete deletes the ConfigMap holding the release named by key.
func (files *Files) Delete(key string) (rls *rspb.Release, err error) {
	// fetch the release to check existence
	if rls, err = files.Get(key); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, storageerrors.ErrReleaseExists(rls.Name)
		}

		files.Log("delete: failed to get release %q: %s", key, err)
		return nil, err
	}
	// delete the release
	if err = os.Remove(files.dir + "/" + key); err != nil {
		return rls, err
	}
	return rls, nil
}

// newConfigMapsObject constructs a kubernetes ConfigMap object
// to store a release. Each configmap data entry is the base64
// encoded string of a release's binary protobuf encoding.
//
// The following labels are used within each configmap:
//
//    "MODIFIED_AT"    - timestamp indicating when this configmap was last modified. (set in Update)
//    "CREATED_AT"     - timestamp indicating when this configmap was created. (set in Create)
//    "VERSION"        - version of the release.
//    "STATUS"         - status of the release (see proto/hapi/release.status.pb.go for variants)
//    "OWNER"          - owner of the configmap, currently "TILLER".
//    "NAME"           - name of the release.
//
func newFile(key string, rls *rspb.Release, lbs labels) (*File, error) {
	const owner = "TILLER"

	// encode the release
	s, err := encodeRelease(rls)
	if err != nil {
		return nil, err
	}

	if lbs == nil {
		lbs.init()
	}

	// apply labels
	lbs.set("NAME", rls.Name)
	lbs.set("OWNER", owner)
	lbs.set("STATUS", rspb.Status_Code_name[int32(rls.Info.Status.Code)])
	lbs.set("VERSION", strconv.Itoa(int(rls.Version)))

	// create and return configmap object
	return &File{
		Labels: lbs.toMap(),
		Data:   s,
	}, nil
}
