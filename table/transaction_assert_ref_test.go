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
	"context"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newAssertRefTestTable builds a writer-side table over the given base
// metadata, backed by a headTrackingCatalog seeded with catalogMeta —
// the catalog state at commit time, which may differ from the writer's
// view to simulate an interleaved commit.
func newAssertRefTestTable(t *testing.T, base, catalogMeta Metadata) (*Table, *headTrackingCatalog) {
	t.Helper()

	cat := &headTrackingCatalog{metadata: catalogMeta}
	tbl := New(Identifier{"db", "assert-ref-test"}, base, "metadata.json",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, cat)

	return tbl, cat
}

// Why: a transaction carrying only metadata updates (e.g. SetProperties
// used for exactly-once bookkeeping) commits with just AssertTableUUID;
// callers need an explicit fence to assert the branch has not moved
// between their read and the commit — and the retry loop's
// refresh-and-replay must not rewrite that fence to the new head, which
// would silently void the compare-and-swap.
// Condition: a properties-only transaction with AssertRefSnapshotID on
// main, committed against a catalog whose branch head did or did not
// move in between.
// Assertion: the commit succeeds when the branch is unmoved and fails
// with ErrCommitFailed — with the properties left uncommitted and no
// retry sneaking the commit through — when it moved.
func TestTransactionAssertRefSnapshotID(t *testing.T) {
	head := int64(100)
	retryProps := iceberg.Properties{
		CommitNumRetriesKey:     "2",
		CommitMinRetryWaitMsKey: "1",
		CommitMaxRetryWaitMsKey: "2",
	}

	t.Run("succeeds when the branch is unmoved", func(t *testing.T) {
		base := newConflictTestMetadataWithProps(t, &head, retryProps)
		tbl, cat := newAssertRefTestTable(t, base, base)

		tx := tbl.NewTransaction()
		require.NoError(t, tx.SetProperties(iceberg.Properties{"offsets": "42"}))
		require.NoError(t, tx.AssertRefSnapshotID(MainBranch))

		_, err := tx.Commit(t.Context())
		require.NoError(t, err)
		assert.Equal(t, int32(1), cat.attempts.Load())
		assert.Equal(t, "42", cat.metadata.Properties()["offsets"])
	})

	t.Run("fails when an interleaved commit moved the branch", func(t *testing.T) {
		base := newConflictTestMetadataWithProps(t, &head, retryProps)
		moved := graftSnapshotOnto(t, base, MainBranch, 200)
		tbl, cat := newAssertRefTestTable(t, base, moved)

		tx := tbl.NewTransaction()
		require.NoError(t, tx.SetProperties(iceberg.Properties{"offsets": "42"}))
		require.NoError(t, tx.AssertRefSnapshotID(MainBranch))

		_, err := tx.Commit(t.Context())
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrCommitFailed)
		assert.NotContains(t, cat.metadata.Properties(), "offsets",
			"a failed compare-and-swap must not apply the properties")
		assert.Equal(t, int32(3), cat.attempts.Load(),
			"every retry must re-submit the pinned assertion and fail — refresh-and-replay must not rewrite the fence")
	})

	t.Run("pins branch absence when the branch does not exist", func(t *testing.T) {
		base := newConflictTestMetadataWithProps(t, nil, retryProps)
		created := newConflictTestMetadataWithProps(t, &head, retryProps)
		tbl, cat := newAssertRefTestTable(t, base, created)

		tx := tbl.NewTransaction()
		require.NoError(t, tx.SetProperties(iceberg.Properties{"offsets": "42"}))
		require.NoError(t, tx.AssertRefSnapshotID(MainBranch))

		_, err := tx.Commit(t.Context())
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrCommitFailed,
			"a branch created concurrently must fail a fence pinned to its absence")
		assert.NotContains(t, cat.metadata.Properties(), "offsets")
	})

	t.Run("empty branch defaults to main", func(t *testing.T) {
		base := newConflictTestMetadataWithProps(t, &head, retryProps)
		moved := graftSnapshotOnto(t, base, MainBranch, 200)
		tbl, _ := newAssertRefTestTable(t, base, moved)

		tx := tbl.NewTransaction()
		require.NoError(t, tx.SetProperties(iceberg.Properties{"offsets": "42"}))
		require.NoError(t, tx.AssertRefSnapshotID(""))

		_, err := tx.Commit(t.Context())
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrCommitFailed)
	})
}

// countRefAssertions returns how many assert-ref-snapshot-id
// requirements the transaction has accumulated.
func countRefAssertions(tx *Transaction) int {
	n := 0
	for _, r := range tx.reqs {
		if _, ok := r.(*assertRefSnapshotID); ok {
			n++
		}
	}

	return n
}

// Why: transaction requirements were deduplicated by requirement TYPE
// alone, so a second AssertRefSnapshotID for a different branch was
// silently discarded and an explicit fence could suppress a producer's
// CAS assertion for its target branch (or vice versa). Dedup must key
// ref assertions by (type, ref) and reject two assertions for the same
// ref that pin different snapshot ids.
// Condition/Assertion: per subtest.
func TestTransactionRequirementDedup(t *testing.T) {
	head := int64(100)
	retryProps := iceberg.Properties{
		CommitNumRetriesKey:     "2",
		CommitMinRetryWaitMsKey: "1",
		CommitMaxRetryWaitMsKey: "2",
	}

	t.Run("independent branches are both enforced", func(t *testing.T) {
		base := newConflictTestMetadataWithProps(t, &head, retryProps)

		// Catalog state: main unmoved, but a peer created the "audit"
		// branch in between — only the audit assertion can catch it.
		builder, err := MetadataBuilderFromBase(base, "")
		require.NoError(t, err)
		require.NoError(t, builder.SetSnapshotRef("audit", head, BranchRef))
		withAudit, err := builder.Build()
		require.NoError(t, err)

		tbl, cat := newAssertRefTestTable(t, base, withAudit)
		tx := tbl.NewTransaction()
		require.NoError(t, tx.SetProperties(iceberg.Properties{"offsets": "42"}))
		require.NoError(t, tx.AssertRefSnapshotID(MainBranch))
		require.NoError(t, tx.AssertRefSnapshotID("audit")) // pins absence
		assert.Equal(t, 2, countRefAssertions(tx),
			"assertions for distinct branches must both be kept")

		_, err = tx.Commit(t.Context())
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrCommitFailed)
		assert.NotContains(t, cat.metadata.Properties(), "offsets")
	})

	t.Run("same ref with the same pinned id dedupes to one", func(t *testing.T) {
		base := newConflictTestMetadataWithProps(t, &head, retryProps)
		tbl, _ := newAssertRefTestTable(t, base, base)

		tx := tbl.NewTransaction()
		require.NoError(t, tx.AssertRefSnapshotID(MainBranch))
		// A producer-built assertion for the same branch pins the same
		// base head; it must coexist with the explicit fence as one
		// requirement.
		require.NoError(t, tx.apply(nil, []Requirement{AssertRefSnapshotID(MainBranch, &head)}))
		assert.Equal(t, 1, countRefAssertions(tx))
	})

	t.Run("same ref with conflicting ids errors", func(t *testing.T) {
		base := newConflictTestMetadataWithProps(t, &head, retryProps)
		tbl, _ := newAssertRefTestTable(t, base, base)

		tx := tbl.NewTransaction()
		require.NoError(t, tx.AssertRefSnapshotID(MainBranch))

		other := int64(200)
		err := tx.apply(nil, []Requirement{AssertRefSnapshotID(MainBranch, &other)})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "conflicting snapshot-id assertions")

		err = tx.apply(nil, []Requirement{AssertRefSnapshotID(MainBranch, nil)})
		require.Error(t, err, "pinned id vs pinned absence must also conflict")
		assert.Contains(t, err.Error(), "conflicting snapshot-id assertions")
	})

	t.Run("explicit fence coexists with a producer commit", func(t *testing.T) {
		dir := t.TempDir()
		schema := iceberg.NewSchema(0,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		)
		props := iceberg.Properties{PropertyFormatVersion: "2"}
		for k, v := range retryProps {
			props[k] = v
		}
		base, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder, dir, props)
		require.NoError(t, err)

		tbl, cat := newAssertRefTestTable(t, base, base)
		tx := tbl.NewTransaction()
		// Explicit fence first: pins main's absence. The fast-append
		// producer's own assertion pins the same base state and must
		// dedupe against it instead of conflicting or being dropped.
		require.NoError(t, tx.AssertRefSnapshotID(MainBranch))
		require.NoError(t, tx.AddDataFiles(t.Context(),
			[]iceberg.DataFile{buildTestDataFileEntry(t, dir+"/data/f1.parquet", iceberg.EntryContentData)}, nil))
		assert.Equal(t, 1, countRefAssertions(tx))

		_, err = tx.Commit(t.Context())
		require.NoError(t, err)
		require.NotNil(t, cat.metadata.CurrentSnapshot(),
			"the fenced producer commit must create the branch")
	})
}
