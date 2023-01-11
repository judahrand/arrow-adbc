package adbcbq

import (
	"context"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
)

type BigqueryStatement struct {
	Conn  *BigqueryConnection
	Query string
}

func NewBigqueryStatement(ctx context.Context, connection *BigqueryConnection) (*adbc.Statement, error) {
	return &BigqueryStatement{Conn: connection}, nil
}

func (s *BigqueryStatement) Close() error {
	return &adbc.Error{Code: adbc.StatusNotImplemented}
}

func (s *BigqueryStatement) SetOption(key, val string) error {
	return &adbc.Error{Code: adbc.StatusNotImplemented}
}

func (s *BigqueryStatement) SetSqlQuery(query string) error {
	s.Query = query
	return nil
}

func (s *BigqueryStatement) ExecuteQuery(ctx context.Context) (array.RecordReader, int64, error) {
	query := s.Conn.BQClient.Query(s.Query)
	job, err := query.Run(ctx)
	if err != nil {
		return nil, -1, err
	}

	_, numRows, err := job.waitForQuery(ctx)
	if err != nil {
		return nil, -1, err
	}

	cfg, err := job.Config()
	if err != nil {
		return nil, err
	}
	qcfg := cfg.(*QueryConfig)
	table := qcfg.Dst

	md, err := table.Metadata(ctx)
	if err != nil {
		return nil, err
	}
	rs, err := table.c.rc.sessionForTable(ctx, table)
	if err != nil {
		return nil, err
	}
	arrowSchema := rs.GetArrowSchema()

	return nil, -1, &adbc.Error{Code: adbc.StatusNotImplemented}
}

func (s *BigqueryStatement) ExecuteUpdate(context.Context) (int64, error) {
	return -1, &adbc.Error{Code: adbc.StatusNotImplemented}
}

func (s *BigqueryStatement) Prepare(context.Context) error {
	return &adbc.Error{Code: adbc.StatusNotImplemented}
}

func (s *BigqueryStatement) SetSubstraitPlan(plan []byte) error {
	return &adbc.Error{Code: adbc.StatusNotImplemented}
}

func (s *BigqueryStatement) Bind(ctx context.Context, values arrow.Record) error {
	return &adbc.Error{Code: adbc.StatusNotImplemented}
}

func (s *BigqueryStatement) BindStream(ctx context.Context, stream array.RecordReader) error {
	return &adbc.Error{Code: adbc.StatusNotImplemented}
}

func (s *BigqueryStatement) GetParameterSchema() (*arrow.Schema, error) {
	return nil, &adbc.Error{Code: adbc.StatusNotImplemented}
}

func (s *BigqueryStatement) ExecutePartitions(context.Context) (*arrow.Schema, adbc.Partitions, int64, error) {
	return nil, adbc.Partitions{}, -1, &adbc.Error{Code: adbc.StatusNotImplemented}
}
