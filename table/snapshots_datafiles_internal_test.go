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

package table

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func buildTestDataFileEntry(t *testing.T, path string, content iceberg.ManifestEntryContent) iceberg.DataFile {
	t.Helper()

	b, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec,
		content, path, iceberg.ParquetFile, nil, nil, nil, 10, 1024)
	require.NoError(t, err)

	return b.Build()
}

func writeSnapshotTestManifest(t *testing.T, fs iceio.WriteFileIO, path string, content iceberg.ManifestContent, files []iceberg.DataFile) iceberg.ManifestFile {
	t.Helper()

	var buf bytes.Buffer
	wr, err := iceberg.NewManifestWriter(2, &buf, *iceberg.UnpartitionedSpec,
		tableSchemaSimple, 1, iceberg.WithManifestWriterContent(content))
	require.NoError(t, err)

	snapshotID := int64(1)
	for _, df := range files {
		require.NoError(t, wr.Add(iceberg.NewManifestEntry(
			iceberg.EntryStatusADDED, &snapshotID, nil, nil, df)))
	}
	require.NoError(t, wr.Close())
	require.NoError(t, fs.WriteFile(path, buf.Bytes()))

	mf, err := wr.ToManifestFile(path, int64(buf.Len()), iceberg.WithManifestFileContent(content))
	require.NoError(t, err)

	return mf
}

// newMultiManifestSnapshot writes nData data manifests of two data files
// each plus one delete manifest with a single position-delete file, and a
// manifest list referencing them in that order. It returns the snapshot
// and the data-file paths in manifest order (delete-file path last).
func newMultiManifestSnapshot(t *testing.T, fs iceio.WriteFileIO, dir string, nData int) (Snapshot, []string) {
	t.Helper()

	require.NoError(t, os.MkdirAll(filepath.Join(dir, "metadata"), 0o755))

	manifests := make([]iceberg.ManifestFile, 0, nData+1)
	paths := make([]string, 0, 2*nData+1)
	for m := range nData {
		files := make([]iceberg.DataFile, 0, 2)
		for n := range 2 {
			p := fmt.Sprintf("%s/data/file-%d-%d.parquet", dir, m, n)
			files = append(files, buildTestDataFileEntry(t, p, iceberg.EntryContentData))
			paths = append(paths, p)
		}
		manifests = append(manifests, writeSnapshotTestManifest(t, fs,
			fmt.Sprintf("%s/metadata/manifest-%d.avro", dir, m),
			iceberg.ManifestContentData, files))
	}

	delPath := dir + "/data/pos-del.parquet"
	manifests = append(manifests, writeSnapshotTestManifest(t, fs,
		dir+"/metadata/manifest-deletes.avro", iceberg.ManifestContentDeletes,
		[]iceberg.DataFile{buildTestDataFileEntry(t, delPath, iceberg.EntryContentPosDeletes)}))
	paths = append(paths, delPath)

	listPath := dir + "/metadata/manifest-list.avro"
	out, err := fs.Create(listPath)
	require.NoError(t, err)
	seq := int64(1)
	require.NoError(t, iceberg.WriteManifestList(2, out, 1, nil, &seq, 0, manifests))
	require.NoError(t, out.Close())

	return Snapshot{SnapshotID: 1, SequenceNumber: 1, ManifestList: listPath}, paths
}

// Why: dataFiles reads manifests concurrently; callers must still see the
// exact sequence a sequential manifest-by-manifest walk would produce.
// Condition: a snapshot with three data manifests and one delete manifest,
// iterated twice with no file filter.
// Assertion: both runs yield every file in manifest order.
func TestSnapshotDataFilesManifestOrder(t *testing.T) {
	fs := iceio.LocalFS{}
	dir := filepath.ToSlash(t.TempDir())
	snap, want := newMultiManifestSnapshot(t, fs, dir, 3)

	for run := range 2 {
		var got []string
		for df, err := range snap.dataFiles(fs, nil) {
			require.NoError(t, err)
			got = append(got, df.FilePath())
		}
		assert.Equal(t, want, got, "run %d must match sequential manifest order", run)
	}
}

// Why: the fileFilter argument prunes by entry content and must keep
// working identically under concurrent manifest reads.
// Condition: iterate the same snapshot filtered to data files only, then
// to position deletes only.
// Assertion: each filter yields exactly the matching subset, in order.
func TestSnapshotDataFilesContentFilter(t *testing.T) {
	fs := iceio.LocalFS{}
	dir := filepath.ToSlash(t.TempDir())
	snap, all := newMultiManifestSnapshot(t, fs, dir, 2)

	var dataOnly []string
	for df, err := range snap.dataFiles(fs, set[iceberg.ManifestEntryContent]{iceberg.EntryContentData: {}}) {
		require.NoError(t, err)
		dataOnly = append(dataOnly, df.FilePath())
	}
	assert.Equal(t, all[:len(all)-1], dataOnly)

	var deletesOnly []string
	for df, err := range snap.dataFiles(fs, set[iceberg.ManifestEntryContent]{iceberg.EntryContentPosDeletes: {}}) {
		require.NoError(t, err)
		deletesOnly = append(deletesOnly, df.FilePath())
	}
	assert.Equal(t, all[len(all)-1:], deletesOnly)
}

// Why: a failed manifest read must surface as an error to the caller, and
// entries from manifests earlier in the list must still be delivered first
// so the failure point is deterministic.
// Condition: delete the second data manifest's file from disk, then iterate.
// Assertion: the first manifest's two files are yielded, then an error.
func TestSnapshotDataFilesManifestReadError(t *testing.T) {
	fs := iceio.LocalFS{}
	dir := filepath.ToSlash(t.TempDir())
	snap, want := newMultiManifestSnapshot(t, fs, dir, 3)
	require.NoError(t, os.Remove(dir+"/metadata/manifest-1.avro"))

	var got []string
	var iterErr error
	for df, err := range snap.dataFiles(fs, nil) {
		if err != nil {
			iterErr = err

			break
		}
		got = append(got, df.FilePath())
	}

	require.Error(t, iterErr)
	assert.Equal(t, want[:2], got, "entries before the failing manifest must be yielded in order")
}

// Why: range-over-func consumers may stop early; the iterator must honor
// that without yielding further entries.
// Condition: break out of the loop after the first yielded file.
// Assertion: exactly one file (the first in manifest order) is observed.
func TestSnapshotDataFilesEarlyBreak(t *testing.T) {
	fs := iceio.LocalFS{}
	dir := filepath.ToSlash(t.TempDir())
	snap, want := newMultiManifestSnapshot(t, fs, dir, 3)

	var got []string
	for df, err := range snap.dataFiles(fs, nil) {
		require.NoError(t, err)
		got = append(got, df.FilePath())

		break
	}

	assert.Equal(t, want[:1], got)
}
