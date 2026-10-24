package core

// Bootstrap is source-owned runner initialization data. Its bytes are never
// shared with callers, and execution providers transport them without
// interpreting them. The zero value is valid empty bootstrap data.
type Bootstrap struct {
	content []byte
}

// NewBootstrap prevents later source mutations from changing an execution request.
func NewBootstrap(content []byte) Bootstrap {
	return Bootstrap{content: append([]byte(nil), content...)}
}

// Content prevents execution code from mutating bootstrap retained by runtime.
func (bootstrap Bootstrap) Content() []byte {
	return append([]byte(nil), bootstrap.content...)
}
