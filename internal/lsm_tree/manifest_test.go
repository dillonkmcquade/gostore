package lsm_tree

import "testing"

func TestNewManifest(t *testing.T) {
	defer CleanAppFiles()
	_, err := NewManifest[int64, string]()
	if err != nil {
		t.Error(err)
	}
}

// func TestManifestAdd(t *testing.T) {}
// func TestManifestAdd(t *testing.T) {}
// func TestManifestAdd(t *testing.T) {}
// func TestManifestAdd(t *testing.T) {}
// func TestManifestAdd(t *testing.T) {}
