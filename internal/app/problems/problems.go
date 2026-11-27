// Package problems maps the errors of the store and of the domain to the
// application errors that a handler shows as a Problem.
package problems

import (
	"errors"

	apperrors "github.com/techorionai/constellation-audit/internal/common/errors"
	"github.com/techorionai/constellation-audit/internal/domain"
	"github.com/techorionai/constellation-audit/internal/ports/entries"
)

// CodeInvalidParameter is the code of a query parameter that breaks a rule.
const CodeInvalidParameter = "invalid_parameter"

// InvalidParameter makes the 400 error for a parameter. The detail names the
// parameter and never holds the value.
func InvalidParameter(param string) error {
	return apperrors.NewBadRequest(CodeInvalidParameter, "The parameter "+param+" is not valid.")
}

// FromStore maps an error of the store. An unavailable store gives 503. A
// refusal of the data (a PostgreSQL data exception on a read) is a bad filter
// value, so it gives 400. Other errors stay as they are, and the response
// layer shows them as 500.
func FromStore(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, entries.ErrUnavailable):
		return apperrors.NewUnavailable()
	case errors.Is(err, entries.ErrDataRefused):
		return apperrors.NewBadRequest(CodeInvalidParameter, "A filter value is not valid.")
	}
	return err
}

// FromFilter maps the error of the filter checks. An error that is not a filter
// error stays as it is.
func FromFilter(err error) error {
	var fe *domain.FilterError
	if errors.As(err, &fe) {
		return InvalidParameter(fe.Param)
	}
	return err
}
