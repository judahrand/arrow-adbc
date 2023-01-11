package adbcbq

import (
	"context"

	"github.com/apache/arrow-adbc/go/adbc"
)

type BigqueryDriver struct{}

func (c *BigqueryDriver) SetOptions(map[string]string) error {
	return &adbc.Error{Code: adbc.StatusNotImplemented}
}

func (c *BigqueryDriver) Open(ctx context.Context) (adbc.Connection, error) {
	return nil, &adbc.Error{Code: adbc.StatusNotImplemented}
}
