package backupedit

import (
	"archive/tar"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/klauspost/compress/s2"

	"github.com/nats-io/jsm.go/api"
)

type preflightInfo struct {
	srcJSONSize  int64
	srcJSONMtime int64
	srcTarSize   int64
	srcTarMtime  int64
}

func preflight(src, dst string) (*preflightInfo, *backupMeta, error) {
	jsonPath := filepath.Join(src, "backup.json")
	tarPath := filepath.Join(src, "stream.tar.s2")

	jsonInfo, err := os.Stat(jsonPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil, ErrSourceNotFound
		}
		return nil, nil, fmt.Errorf("stat %s: %w", jsonPath, err)
	}
	tarInfo, err := os.Stat(tarPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil, ErrSourceNotFound
		}
		return nil, nil, fmt.Errorf("stat %s: %w", tarPath, err)
	}

	meta, err := readBackupJSON(jsonPath)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %s", ErrInvalidBackup, err.Error())
	}

	if err := probeTarFormat(tarPath); err != nil {
		return nil, nil, err
	}
	if err := checkDestination(dst); err != nil {
		return nil, nil, err
	}

	return &preflightInfo{
		srcJSONSize:  jsonInfo.Size(),
		srcJSONMtime: jsonInfo.ModTime().UnixNano(),
		srcTarSize:   tarInfo.Size(),
		srcTarMtime:  tarInfo.ModTime().UnixNano(),
	}, meta, nil
}

func readBackupJSON(path string) (*backupMeta, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m backupMeta
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

func probeTarFormat(tarPath string) error {
	f, err := os.Open(tarPath)
	if err != nil {
		return fmt.Errorf("open %s: %w", tarPath, err)
	}
	defer f.Close()
	tr := tar.NewReader(s2.NewReader(f))
	hdr, err := tr.Next()
	if err != nil {
		if err == io.EOF {
			return ErrInvalidBackup
		}
		return fmt.Errorf("%w: reading first tar entry: %s", ErrInvalidBackup, err.Error())
	}
	switch hdr.Name {
	case "state.json":
		return nil
	case "meta.inf":
		return ErrUnsupportedV1Format
	default:
		return fmt.Errorf("%w: unexpected first tar entry %q", ErrInvalidBackup, hdr.Name)
	}
}

func checkDestination(dst string) error {
	info, err := os.Stat(dst)
	switch {
	case err == nil:
		if !info.IsDir() {
			return fmt.Errorf("%w: %s is not a directory", ErrDestinationExists, dst)
		}
		entries, rerr := os.ReadDir(dst)
		if rerr != nil {
			return fmt.Errorf("read dst dir: %w", rerr)
		}
		if len(entries) > 0 {
			return ErrDestinationExists
		}
	case errors.Is(err, fs.ErrNotExist):
		// fine
	default:
		return fmt.Errorf("stat dst: %w", err)
	}

	parent := filepath.Dir(dst)
	base := filepath.Base(dst)
	entries, err := os.ReadDir(parent)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("read dst parent: %w", err)
	}
	prefix := base + ".tmp-"
	for _, e := range entries {
		if len(e.Name()) > len(prefix) && e.Name()[:len(prefix)] == prefix {
			return fmt.Errorf("%w: stale %s", ErrDestinationExists, filepath.Join(parent, e.Name()))
		}
	}
	return nil
}

// backupMeta is the on-disk shape of backup.json — jsm.go's sidecar.
type backupMeta struct {
	Config    api.StreamConfig `json:"config"`
	State     api.StreamState  `json:"state"`
	MutatedBy string           `json:"mutated_by,omitempty"`
}
