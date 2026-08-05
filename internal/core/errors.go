package core

import "errors"

var ErrConflictingResult = errors.New("job already has a different result")
