package fq

type (
	Writer struct{}
	Reader struct{}
)

func NewWriter(path string) (*Writer, error) {
	return &Writer{}, nil
}

func (w *Writer) Append(msg []byte) (offset uint64, err error) {
	return 0, nil
}

func (w *Writer) Flush(offset uint64) error {
	return nil
}

func (w *Writer) Close() {}

func NewReader(path string, offset uint64) (*Reader, error) {
	return &Reader{}, nil
}

func (r *Reader) Read() (msg []byte, err error) {
	return nil, nil
}

func (r *Reader) Offset() uint64 {
	return 0
}

func (r *Reader) Close() {}
