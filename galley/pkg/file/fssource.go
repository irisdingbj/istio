//  Copyright 2018 Istio Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package file

import (
	"bytes"
	"crypto/sha1"
	"encoding/json"

	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"time"

	"github.com/ghodss/yaml"
	"github.com/gogo/protobuf/proto"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"istio.io/istio/pkg/log"

	"istio.io/istio/galley/pkg/kube"
	kube_meta "istio.io/istio/galley/pkg/metadata/kube"
	"istio.io/istio/galley/pkg/runtime"
	"istio.io/istio/galley/pkg/runtime/resource"
)

var supportedExtensions = map[string]bool{
	".yaml": true,
	".yml":  true,
}
var scope = log.RegisterScope("file-source", "Source for File System", 0)

type fsStore struct {
	*backEndResource
	sha [sha1.Size]byte
}

// resourceMeta is the standard metadata associated with a resource.
type resourceMeta struct {
	Name        string
	Namespace   string
	Labels      map[string]string
	Annotations map[string]string
	Revision    string
}

// backEndResource represents a resources with a raw spec
type backEndResource struct {
	Kind     string
	Metadata resourceMeta
	Spec     map[string]interface{}
}

// key returns the key of the resource in the store.
func (ber *backEndResource) key() string {
	if len(ber.Metadata.Namespace) > 0 {
		return ber.Metadata.Namespace + "/" + ber.Metadata.Name
	}
	return ber.Metadata.Name
}

func (r *fsStore) UnmarshalJSON(bytes []byte) error {
	return json.Unmarshal(bytes, &r.backEndResource)
}

// fsSource is source implementation for filesystem.
type fsSource struct {
	root          string
	checkDuration time.Duration
	donec         chan struct{}
	mu            sync.RWMutex
	data          map[string]*fsStore
	versions      map[string]uint64
	ch            chan resource.Event
	kinds         map[string]bool
}

func parseFile(path string, data []byte) []*fsStore {
	chunks := bytes.Split(data, []byte("\n---\n"))
	resources := make([]*fsStore, 0, len(chunks))
	for i, chunk := range chunks {
		chunk = bytes.TrimSpace(chunk)
		if len(chunk) == 0 {
			continue
		}
		r, err := parseChunk(chunk)
		if err != nil {
			scope.Errorf("Error processing %s[%d]: %v", path, i, err)
			continue
		}
		if r == nil {
			continue
		}
		resources = append(resources, &fsStore{backEndResource: r, sha: sha1.Sum(chunk)})
	}
	return resources
}

func parseChunk(chunk []byte) (*backEndResource, error) {
	r := &backEndResource{}
	if err := yaml.Unmarshal(chunk, r); err != nil {
		return nil, err
	}
	if empty(r) {
		return nil, nil
	}
	return r, nil
}

var emptyResource = &backEndResource{}

// Check if the parsed resource is empty
func empty(r *backEndResource) bool {
	return reflect.DeepEqual(*r, *emptyResource)
}

func (s *fsSource) readFiles() map[string]*fsStore {
	result := map[string]*fsStore{}

	err := filepath.Walk(s.root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if mode := info.Mode() & os.ModeType; !supportedExtensions[filepath.Ext(path)] || (mode != 0 && mode != os.ModeSymlink) {
			return nil
		}
		data, err := ioutil.ReadFile(path)
		if err != nil {
			scope.Warnf("Failed to read %s: %v", path, err)
			return err
		}
		for _, r := range parseFile(path, data) {
			if !s.kinds[r.Kind] {
				continue
			}
			result[r.key()] = r
		}
		return nil
	})
	if err != nil {
		scope.Errorf("failure during filepath.Walk: %v", err)
	}
	return result
}

func (s *fsSource) checkAndUpdate() {
	newData := s.readFiles()
	removed := map[string]bool{}
	s.mu.Lock()
	for k := range s.data {
		removed[k] = true
	}
	for k, r := range newData {
		oldRes, ok := s.data[k]
		if !ok {
			s.versions[k] = 1
			s.process(resource.Added, k, r)
			continue
		}
		delete(removed, k)
		if r.sha != oldRes.sha {
			s.versions[k]++
			s.process(resource.Updated, k, r)
		}
	}
	for k := range removed {
		s.process(resource.Deleted, k, s.data[k])
		delete(s.versions, k)
	}
	s.data = newData
	s.mu.Unlock()
}

func (s *fsSource) Stop() {
	close(s.donec)
	s.donec = nil
}

func (s *fsSource) process(kind resource.EventKind, key string, r *fsStore) {
	var item proto.Message
	var err error
	var createTime time.Time
	u := &unstructured.Unstructured{
		Object: r.backEndResource.Spec,
	}
	for _, spec := range kube_meta.Types.All() {
		if spec.Kind == r.Kind {
			if key, createTime, item, err = spec.Converter(spec.Target, key, u); err != nil {
				scope.Errorf("Unable to convert unstructured to proto: %s/%s", key, s.versions[key])
				return
			}
			break
		}
	}
	targetInfo := getTargetInfo(r.Kind)
	rid := resource.VersionedKey{
		Key: resource.Key{
			TypeURL:  targetInfo.TypeURL,
			FullName: key,
		},
		Version:    resource.Version(s.versions[key]),
		CreateTime: createTime,
	}
	e := resource.Event{
		ID:   rid,
		Kind: kind,
		Item: item,
	}
	scope.Debugf("Dispatching source event: %v", e)
	s.ch <- e
}

// Start implements runtime.Source
func (s *fsSource) Start() (chan resource.Event, error) {
	s.ch = make(chan resource.Event, 1024)
	s.checkAndUpdate()
	go func() {
		tick := time.NewTicker(s.checkDuration)
		for {
			select {
			case <-s.donec:
				tick.Stop()
				return
			case <-tick.C:
				s.checkAndUpdate()
			}
		}
	}()
	return s.ch, nil
}

// New returns a File System implementation of runtime.Source.
func New(root string, resyncPeriod time.Duration) (runtime.Source, error) {
	return newFsSource(root, resyncPeriod, kube_meta.Types.All())
}

func newFsSource(root string, resyncPeriod time.Duration, specs []kube.ResourceSpec) (runtime.Source, error) {
	fs := &fsSource{
		root:          root,
		kinds:         map[string]bool{},
		checkDuration: resyncPeriod,
		data:          map[string]*fsStore{},
		donec:         make(chan struct{}),
		versions:      map[string]uint64{},
	}
	for _, spec := range specs {
		fs.kinds[spec.Kind] = true
	}
	return fs, nil
}
func getTargetInfo(kind string) (info *resource.Info) {
	for _, spec := range kube_meta.Types.All() {
		if spec.Kind == kind {
			return &spec.Target
		}
	}
	return nil

}
