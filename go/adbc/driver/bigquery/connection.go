package adbcbq

import (
	"context"

	"cloud.google.com/go/bigquery"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
)

type BigqueryConnection struct {
	BQClient *bigquery.Client
}

func NewBigqueryConnection(ctx context.Context, projectID string) (*BigqueryConnection, error) {
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return nil, err
	}
	return &BigqueryConnection{BQClient: client}, nil
}

func (c *BigqueryConnection) GetInfo(ctx context.Context, infoCodes []adbc.InfoCode) (array.RecordReader, error) {
	return nil, &adbc.Error{Code: adbc.StatusNotImplemented}
}

func (c *BigqueryConnection) GetObjects(ctx context.Context, depth adbc.ObjectDepth, catalog, dbSchema, tableName, columnName *string, tableType []string) (array.RecordReader, error) {
	return nil, &adbc.Error{Code: adbc.StatusNotImplemented}
}

func (c *BigqueryConnection) GetTableSchema(ctx context.Context, catalog, dbSchema *string, tableName string) (*arrow.Schema, error) {
	return nil, &adbc.Error{Code: adbc.StatusNotImplemented}
}

func (c *BigqueryConnection) GetTableTypes(context.Context) (array.RecordReader, error) {
	return nil, &adbc.Error{Code: adbc.StatusNotImplemented}
}

func (c *BigqueryConnection) Commit(context.Context) error {
	return &adbc.Error{Code: adbc.StatusNotImplemented}
}

func (c *BigqueryConnection) Rollback(context.Context) error {
	return &adbc.Error{Code: adbc.StatusNotImplemented}
}

func (c *BigqueryConnection) NewStatement() (BigqueryStatement, error) {
	return BigqueryStatement{}, &adbc.Error{Code: adbc.StatusNotImplemented}
}

func (c *BigqueryConnection) Close() error {
	return &adbc.Error{Code: adbc.StatusNotImplemented}
}

func (c *BigqueryConnection) ReadPartition(ctx context.Context, serializedPartition []byte) (array.RecordReader, error) {
	return nil, &adbc.Error{Code: adbc.StatusNotImplemented}
}
