// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Package bigquery is an ADBC Driver Implementation for Google Bigquery
// natively in go.
//
// It can be used to register a driver for database/sql by importing
// github.com/apache/arrow-adbc/go/adbc/sqldriver and running:
//
//	sql.Register("bigquery", sqldriver.Driver{bigquery.Driver{}})
//
// You can then open a bigquery connection with the database/sql
// standard package by using:
//
//	db, err := sql.Open("bigquery", "projectID=<project_id>")
package bigquery

import (
	"context"
	"fmt"
	"runtime/debug"
	"strings"

	"cloud.google.com/go/bigquery"
	bqStorage "cloud.google.com/go/bigquery/storage/apiv1"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/flight"
	"github.com/apache/arrow/go/v10/arrow/flight/flightsql"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"golang.org/x/exp/maps"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

const (
	OptionProjectID = "adbc.bigquery.project_id"

	infoDriverName = "ADBC Bigquery Driver - Go"
)

var (
	infoDriverVersion      string
	infoDriverArrowVersion string
)

func init() {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, dep := range info.Deps {
			switch {
			case dep.Path == "github.com/apache/arrow-adbc/go/adbc/driver/bigquery":
				infoDriverVersion = dep.Version
			case strings.HasPrefix(dep.Path, "github.com/apache/arrow/go/"):
				infoDriverArrowVersion = dep.Version
			}
		}
	}
}

type Driver struct {
	Alloc memory.Allocator
}

func (d Driver) NewDatabase(opts map[string]string) (adbc.Database, error) {
	db := &database{alloc: &d.Alloc}
	if db.alloc == nil {
		db.alloc = &memory.DefaultAllocator
	}

	return db, db.SetOptions(opts)
}

type database struct {
	projectID string

	alloc *memory.Allocator
}

func (d *database) SetOptions(cnOptions map[string]string) error {
	if val, ok := cnOptions[OptionProjectID]; ok {
		d.projectID = val
	} else {
		d.projectID = bigquery.DetectProjectID
	}

	return nil
}

func getBigqueryClient(ctx context.Context, d *database) (*bigquery.Client, error) {
	cl, err := bigquery.NewClient(ctx, d.projectID)
	if err != nil {
		return nil, adbc.Error{
			Msg:  err.Error(),
			Code: adbc.StatusIO,
		}
	}

	return cl, nil
}

func getBigqueryStorageClient(ctx context.Context, d *database) (*bqStorage.BigQueryReadClient, error) {
	cl, err := bqStorage.NewBigQueryReadClient(ctx)
	if err != nil {
		return nil, adbc.Error{
			Msg:  err.Error(),
			Code: adbc.StatusIO,
		}
	}

	return cl, nil
}

type clientConn struct {
	command_cl *bigquery.Client
	read_cl    *bqStorage.BigQueryReadClient
}

func (d *database) Open(ctx context.Context) (adbc.Connection, error) {
	command_cl, err := getBigqueryClient(ctx, d)
	if err != nil {
		return nil, err
	}

	read_cl, err := getBigqueryStorageClient(ctx, d)
	if err != nil {
		return nil, err
	}

	return &cnxn{command_cl: command_cl, read_cl: read_cl, db: d}, nil
}

type cnxn struct {
	command_cl *bigquery.Client
	read_cl    *bqStorage.BigQueryReadClient

	db *database
}

var adbcToBigqueryInfo = map[adbc.InfoCode]string{
	adbc.InfoVendorName: "Bigquery",
}

// GetInfo returns metadata about the database/driver.
//
// The result is an Arrow dataset with the following schema:
//
//	Field Name					| Field Type
//	----------------------------|-----------------------------
//	info_name					| uint32 not null
//	info_value					| INFO_SCHEMA
//
// INFO_SCHEMA is a dense union with members:
//
//	Field Name (Type Code)		| Field Type
//	----------------------------|-----------------------------
//	string_value (0)			| utf8
//	bool_value (1)				| bool
//	int64_value (2)				| int64
//	int32_bitmask (3)			| int32
//	string_list (4)				| list<utf8>
//	int32_to_int32_list_map (5)	| map<int32, list<int32>>
//
// Each metadatum is identified by an integer code. The recognized
// codes are defined as constants. Codes [0, 10_000) are reserved
// for ADBC usage. Drivers/vendors will ignore requests for unrecognized
// codes (the row will be omitted from the result).
func (c *cnxn) GetInfo(ctx context.Context, infoCodes []adbc.InfoCode) (array.RecordReader, error) {
	const strValTypeID arrow.UnionTypeCode = 0

	if len(infoCodes) == 0 {
		infoCodes = maps.Keys(adbcToBigqueryInfo)
	}

	bldr := array.NewRecordBuilder(*c.db.alloc, adbc.GetInfoSchema)
	defer bldr.Release()
	bldr.Reserve(len(infoCodes))

	infoNameBldr := bldr.Field(0).(*array.Uint32Builder)
	infoValueBldr := bldr.Field(1).(*array.DenseUnionBuilder)
	strInfoBldr := infoValueBldr.Child(0).(*array.StringBuilder)

	translated := make([]flightsql.SqlInfo, 0, len(infoCodes))
	for _, code := range infoCodes {
		if t, ok := adbcToBigqueryInfo[code]; ok {
			translated = append(translated, t)
			continue
		}

		switch code {
		case adbc.InfoDriverName:
			infoNameBldr.Append(uint32(code))
			infoValueBldr.Append(strValTypeID)
			strInfoBldr.Append(infoDriverName)
		case adbc.InfoDriverVersion:
			infoNameBldr.Append(uint32(code))
			infoValueBldr.Append(strValTypeID)
			strInfoBldr.Append(infoDriverVersion)
		case adbc.InfoDriverArrowVersion:
			infoNameBldr.Append(uint32(code))
			infoValueBldr.Append(strValTypeID)
			strInfoBldr.Append(infoDriverArrowVersion)
		}
	}

	final := bldr.NewRecord()
	defer final.Release()
	return array.NewRecordReader(adbc.GetInfoSchema, []arrow.Record{final})
}

// GetObjects gets a hierarchical view of all catalogs, database schemas,
// tables, and columns.
//
// The result is an Arrow Dataset with the following schema:
//
//	Field Name					| Field Type
//	----------------------------|----------------------------
//	catalog_name				| utf8
//	catalog_db_schemas			| list<DB_SCHEMA_SCHEMA>
//
// DB_SCHEMA_SCHEMA is a Struct with the fields:
//
//	Field Name					| Field Type
//	----------------------------|----------------------------
//	db_schema_name				| utf8
//	db_schema_tables			|	list<TABLE_SCHEMA>
//
// TABLE_SCHEMA is a Struct with the fields:
//
//	Field Name					| Field Type
//	----------------------------|----------------------------
//	table_name					| utf8 not null
//	table_type					|	utf8 not null
//	table_columns				| list<COLUMN_SCHEMA>
//	table_constraints	 		| list<CONSTRAINT_SCHEMA>
//
// COLUMN_SCHEMA is a Struct with the fields:
//
//		Field Name					| Field Type		  | Comments
//		----------------------------|---------------------|---------
//		column_name					| utf8 not null		  |
//		ordinal_position			| int32				  | (1)
//		remarks						| utf8				  | (2)
//		xdbc_data_type				| int16				  | (3)
//		xdbc_type_name				| utf8				  | (3)
//		xdbc_column_size			| int32				  | (3)
//		xdbc_decimal_digits			| int16				  | (3)
//		xdbc_num_prec_radix			| int16				  | (3)
//		xdbc_nullable				| int16				  | (3)
//		xdbc_column_def				| utf8				  | (3)
//		xdbc_sql_data_type			| int16				  | (3)
//		xdbc_datetime_sub			| int16				  | (3)
//		xdbc_char_octet_length		| int32				  | (3)
//		xdbc_is_nullable			| utf8				  | (3)
//		xdbc_scope_catalog			| utf8				  | (3)
//		xdbc_scope_schema			| utf8				  | (3)
//		xdbc_scope_table			| utf8				  | (3)
//		xdbc_is_autoincrement		| bool				  | (3)
//		xdbc_is_generatedcolumn		| bool				  | (3)
//
//	 1. The column's ordinal position in the table (starting from 1).
//	 2. Database-specific description of the column.
//	 3. Optional Value. Should be null if not supported by the driver.
//	    xdbc_values are meant to provide JDBC/ODBC-compatible metadata
//	    in an agnostic manner.
//
// CONSTRAINT_SCHEMA is a Struct with the fields:
//
//	Field Name					| Field Type		  | Comments
//	----------------------------|---------------------|---------
//	constraint_name				| utf8				  |
//	constraint_type				| utf8 not null		  | (1)
//	constraint_column_names		| list<utf8> not null | (2)
//	constraint_column_usage		| list<USAGE_SCHEMA>  | (3)
//
// 1. One of 'CHECK', 'FOREIGN KEY', 'PRIMARY KEY', or 'UNIQUE'.
// 2. The columns on the current table that are constrained, in order.
// 3. For FOREIGN KEY only, the referenced table and columns.
//
// USAGE_SCHEMA is a Struct with fields:
//
//	Field Name					|	Field Type
//	----------------------------|----------------------------
//	fk_catalog					| utf8
//	fk_db_schema				| utf8
//	fk_table					| utf8 not null
//	fk_column_name				| utf8 not null
//
// For the parameters: If nil is passed, then that parameter will not
// be filtered by at all. If an empty string, then only objects without
// that property (ie: catalog or db schema) will be returned.
//
// tableName and columnName must be either nil (do not filter by
// table name or column name) or non-empty.
//
// All non-empty, non-nil strings should be a search pattern (as described
// earlier).
func (c *cnxn) GetObjects(ctx context.Context, depth adbc.ObjectDepth, catalog *string, dbSchema *string, tableName *string, columnName *string, tableType []string) (array.RecordReader, error) {
	panic("not implemented") // TODO: Implement
}

func (c *cnxn) GetTableSchema(ctx context.Context, catalog *string, dbSchema *string, tableName string) (*arrow.Schema, error) {
	// Add a RowRestriction to ensure that we don't return actual data.
	tableReadOptions := &bqStorage.ReadSession_TableReadOptions{
		RowRestriction: `0 = 1`,
	}

	createReadSessionRequest := &bqStorage.CreateReadSessionRequest{
		Parent: fmt.Sprintf("projects/%s", *catalog),
		ReadSession: &bqStorage.ReadSession{
			Table:       tableName,
			DataFormat:  bqStorage.DataFormat_ARROW,
			ReadOptions: tableReadOptions,
		},
		MaxStreamCount: 1,
	}

	// Create the session from the request.
	session, err := c.read_cl.CreateReadSession(ctx, createReadSessionRequest, rpcOpts)
	return nil, adbc.Error{
		Msg:  err.Error(),
		Code: adbc.StatusIO,
	}

	schemaBytes := session.GetArrowSchema().GetSerializedSchema()
	s, err := deserializeArrowSchema(schemaBytes, c.db.alloc)
	return nil, adbc.Error{
		Msg:  err.Error(),
		Code: adbc.StatusInternal,
	}
	return s, nil
}

// GetTableTypes returns a list of the table types in the database.
//
// The result is an arrow dataset with the following schema:
//
//	Field Name			| Field Type
//	----------------|--------------
//	table_type			| utf8 not null
func (c *cnxn) GetTableTypes(ctx context.Context) (array.RecordReader, error) {
	info, err := c.cl.GetTableTypes(ctx)
	if err != nil {
		return nil, adbcFromFlightStatus(err)
	}

	return newRecordReader(ctx, c.db.alloc, c.cl, info, c.clientCache)
}

// Commit commits any pending transactions on this connection, it should
// only be used if autocommit is disabled.
//
// Behavior is undefined if this is mixed with SQL transaction statements.
func (c *cnxn) Commit(_ context.Context) error {
	return adbc.Error{
		Msg:  "[Flight SQL] Transaction methods are not implemented yet",
		Code: adbc.StatusNotImplemented}
}

// Rollback rolls back any pending transactions. Only used if autocommit
// is disabled.
//
// Behavior is undefined if this is mixed with SQL transaction statements.
func (c *cnxn) Rollback(_ context.Context) error {
	return adbc.Error{
		Msg:  "[Flight SQL] Transaction methods are not implemented yet",
		Code: adbc.StatusNotImplemented}
}

// NewStatement initializes a new statement object tied to this connection
func (c *cnxn) NewStatement() (adbc.Statement, error) {
	return &statement{
		alloc:       c.db.alloc,
		cl:          c.cl,
		clientCache: c.clientCache,
		authToken:   c.authToken,
	}, nil
}

// Close closes this connection and releases any associated resources.
func (c *cnxn) Close() error {
	return c.cl.Close()
}

// ReadPartition constructs a statement for a partition of a query. The
// results can then be read independently using the returned RecordReader.
//
// A partition can be retrieved by using ExecutePartitions on a statement.
func (c *cnxn) ReadPartition(ctx context.Context, serializedPartition []byte) (rdr array.RecordReader, err error) {
	var endpoint flight.FlightEndpoint
	if err := proto.Unmarshal(serializedPartition, &endpoint); err != nil {
		return nil, adbc.Error{
			Msg:  err.Error(),
			Code: adbc.StatusInvalidArgument,
		}
	}

	ctx = metadata.AppendToOutgoingContext(ctx, "Authorization", c.authToken)
	rdr, err = doGet(ctx, c.cl, &endpoint, c.clientCache)
	if err != nil {
		return nil, adbcFromFlightStatus(err)
	}
	return rdr, nil
}
