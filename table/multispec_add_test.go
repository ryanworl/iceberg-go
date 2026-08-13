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

package table_test

import (
	"context"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

// TestAddDataFilesMultipleSpecs pins that a single commit may add data
// files targeting any partition spec registered in the table metadata,
// with one manifest written per spec — the shape Java's
// MergingSnapshotProducer supports. This is what allows writers to keep
// producing under a previous spec after an evolution and rewrites to
// migrate files between specs.
func TestAddDataFilesMultipleSpecs(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	fs := iceio.LocalFS{}

	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	)
	identitySpec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1},
		FieldID:   iceberg.PartitionDataIDStart,
		Name:      "id",
		Transform: iceberg.IdentityTransform{},
	})

	meta, err := table.NewMetadata(schema, &identitySpec, table.UnsortedSortOrder, dir, nil)
	require.NoError(t, err)
	cat := &mergeCatalog{meta: meta}
	tbl := table.New(table.Identifier{"default", "multispec"}, meta, dir+"/metadata/00000.json",
		func(_ context.Context) (iceio.IO, error) { return fs, nil }, cat)

	// Evolve the default to unpartitioned; the identity spec 0 stays registered.
	txn := tbl.NewTransaction()
	require.NoError(t, txn.UpdateSpec(false).RemoveField("id").Commit())
	tbl, err = txn.Commit(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, tbl.Metadata().DefaultPartitionSpec())
	require.Len(t, tbl.Metadata().PartitionSpecs(), 2)

	newFile := func(spec iceberg.PartitionSpec, path string, partition map[int]any) iceberg.DataFile {
		bldr, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData,
			path, iceberg.ParquetFile, partition, nil, nil, 1, 100)
		require.NoError(t, err)

		return bldr.Build()
	}
	oldSpec := tbl.Metadata().PartitionSpecs()[0]
	newSpec := tbl.Metadata().PartitionSpecs()[1]
	fileOld := newFile(oldSpec, dir+"/data/old-spec.parquet",
		map[int]any{iceberg.PartitionDataIDStart: int32(7)})
	fileNew := newFile(newSpec, dir+"/data/new-spec.parquet", map[int]any{})

	txn = tbl.NewTransaction()
	require.NoError(t, txn.AddDataFiles(ctx, []iceberg.DataFile{fileOld, fileNew}, nil))
	tbl, err = txn.Commit(ctx)
	require.NoError(t, err)

	manifests, err := tbl.CurrentSnapshot().Manifests(fs)
	require.NoError(t, err)
	require.Len(t, manifests, 2)

	bySpec := map[int32]string{}
	for _, m := range manifests {
		entries, err := m.FetchEntries(fs, false)
		require.NoError(t, err)
		require.Len(t, entries, 1)
		bySpec[m.PartitionSpecID()] = entries[0].DataFile().FilePath()
		require.Equal(t, m.PartitionSpecID(), entries[0].DataFile().SpecID())
	}
	require.Equal(t, dir+"/data/old-spec.parquet", bySpec[0])
	require.Equal(t, dir+"/data/new-spec.parquet", bySpec[1])

	// Partition tuples survive the round trip under each manifest's own spec.
	require.Equal(t, int32(7), func() any {
		for _, m := range manifests {
			if m.PartitionSpecID() != 0 {
				continue
			}
			entries, err := m.FetchEntries(fs, false)
			require.NoError(t, err)

			return entries[0].DataFile().Partition()[iceberg.PartitionDataIDStart]
		}

		return nil
	}())

	// An unregistered spec id is still rejected.
	ghost := iceberg.NewPartitionSpecID(99)
	txn = tbl.NewTransaction()
	err = txn.AddDataFiles(ctx, []iceberg.DataFile{
		newFile(ghost, dir+"/data/ghost.parquet", map[int]any{}),
	}, nil)
	require.ErrorContains(t, err, "unregistered partition spec id 99")
}
