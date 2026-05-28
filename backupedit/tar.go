package backupedit

import (
	"archive/tar"
	"os"

	"github.com/klauspost/compress/s2"
)

type tarReader struct {
	file *os.File
	s2r  *s2.Reader
	tr   *tar.Reader
}

func openTar(path string) (*tarReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	s2r := s2.NewReader(f)
	return &tarReader{file: f, s2r: s2r, tr: tar.NewReader(s2r)}, nil
}

func (r *tarReader) Next() (*tar.Header, error) { return r.tr.Next() }
func (r *tarReader) Read(p []byte) (int, error) { return r.tr.Read(p) }
func (r *tarReader) Close() error {
	if r == nil || r.file == nil {
		return nil
	}
	err := r.file.Close()
	r.file = nil
	return err
}

type tarWriter struct {
	file *os.File
	s2w  *s2.Writer
	tw   *tar.Writer
}

func createTar(path string) (*tarWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	s2w := s2.NewWriter(f)
	return &tarWriter{file: f, s2w: s2w, tw: tar.NewWriter(s2w)}, nil
}

func (w *tarWriter) WriteHeader(h *tar.Header) error { return w.tw.WriteHeader(h) }
func (w *tarWriter) Write(p []byte) (int, error)     { return w.tw.Write(p) }
func (w *tarWriter) Close() error {
	if w == nil || w.file == nil {
		return nil
	}
	tarErr := w.tw.Close()
	s2Err := w.s2w.Close()
	fileErr := w.file.Close()
	w.file = nil
	if tarErr != nil {
		return tarErr
	}
	if s2Err != nil {
		return s2Err
	}
	return fileErr
}
