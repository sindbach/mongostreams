package mongostreams

import (
	"github.com/sbinet/go-python"
)

type PyActions struct {
	OnInsert     *python.PyObject
	OnUpdate     *python.PyObject
	OnReplace    *python.PyObject
	OnDelete     *python.PyObject
	OnInvalidate *python.PyObject
}
