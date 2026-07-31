package remoterepo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"go.kenn.io/kit/backup"
	"go.kenn.io/kit/pack"

	"go.kenn.io/msgvault/internal/objstore"
)

// Repo is the read surface both repository backends satisfy: the kit-backed
// filesystem Reader and the objstore-backed ObjectReader.
type Repo interface {
	RepoID() string
	LatestSnapshot() (*backup.Manifest, error)
	Has(hash string) (bool, error)
	Refresh() error
	OpenBlob(ctx context.Context, hash string) (io.ReadCloser, int64, error)
	Close() error
}

var (
	_ Repo = (*Reader)(nil)
	_ Repo = (*ObjectReader)(nil)
)

// repoIDPattern mirrors kit's canonical repository ID shape (a
// lowercase-hex UUID); anything else in config.toml is tampering.
var repoIDPattern = regexp.MustCompile(
	`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// ObjectReader resolves content hashes against a backup repository served
// through an objstore.Store (S3 or any ranged-read object source). It is
// read-only and lock-free for the same reasons as Reader; every pack open
// costs three ranged reads (header, trailer, footer) and each blob one
// more, with no descriptor to hold between reads.
type ObjectReader struct {
	store objstore.Store
	cfg   backup.RepoConfig

	mu        sync.Mutex
	perFile   map[string][]backup.IndexEntry // folded per index file, cache key = object key
	index     map[pack.BlobID]backup.IndexEntry
	packSizes map[string]int64
}

// OpenObjectStore reads and validates the repository's config.toml through
// store, enforcing the same compatibility rules as kit's backup.Open:
// reader version, plain encryption, canonical repo ID.
func OpenObjectStore(ctx context.Context, store objstore.Store) (*ObjectReader, error) {
	data, err := store.ReadAll(ctx, "config.toml")
	if err != nil {
		return nil, fmt.Errorf("open object repository: reading config.toml: %w", err)
	}
	var cfg backup.RepoConfig
	if err := toml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("open object repository: parsing config.toml: %w", err)
	}
	if cfg.MinReaderVersion > backup.SupportedReaderVersion {
		return nil, fmt.Errorf(
			"open object repository: repository requires reader version %d but this reader supports %d; upgrade msgvault",
			cfg.MinReaderVersion, backup.SupportedReaderVersion)
	}
	if cfg.Encryption != "none" {
		return nil, fmt.Errorf(
			"open object repository: encrypted repositories are not supported yet (encryption=%q)",
			cfg.Encryption)
	}
	if !repoIDPattern.MatchString(cfg.RepoID) {
		return nil, fmt.Errorf(
			"open object repository: repo_id %q is not the canonical form; refusing a tampered config.toml",
			cfg.RepoID)
	}
	return &ObjectReader{
		store:     store,
		cfg:       cfg,
		perFile:   map[string][]backup.IndexEntry{},
		packSizes: map[string]int64{},
	}, nil
}

// RepoID returns the repository's stable identity.
func (r *ObjectReader) RepoID() string { return r.cfg.RepoID }

// LatestSnapshot returns the newest snapshot manifest, or nil when the
// repository has none. Snapshot IDs begin with a UTC second timestamp, so
// lexicographic order is chronological.
func (r *ObjectReader) LatestSnapshot() (*backup.Manifest, error) {
	keys, err := r.store.List(context.Background(), "snapshots/")
	if err != nil {
		return nil, fmt.Errorf("list snapshots: %w", err)
	}
	newest := ""
	for _, key := range keys {
		if strings.HasSuffix(key, ".mvmanifest") && key > newest {
			newest = key
		}
	}
	if newest == "" {
		return nil, nil
	}
	data, err := r.store.ReadAll(context.Background(), newest)
	if err != nil {
		return nil, fmt.Errorf("read snapshot %s: %w", newest, err)
	}
	var m backup.Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("parse snapshot %s: %w", newest, err)
	}
	// The same tamper-evidence check LoadManifest performs: the snapshot ID
	// must recompute from the manifest content.
	createdAt, err := time.Parse(time.RFC3339, m.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("snapshot %s: bad created_at: %w", newest, err)
	}
	id, err := backup.ComputeSnapshotID(createdAt, &m)
	if err != nil {
		return nil, err
	}
	if id != m.SnapshotID {
		return nil, fmt.Errorf("snapshot %s: manifest content does not reproduce its snapshot ID", newest)
	}
	return &m, nil
}

// Has reports whether hash resolves through the loaded index union,
// loading it on first use without reloading on absence (see Reader.Has).
func (r *ObjectReader) Has(hash string) (bool, error) {
	id, err := pack.ParseBlobID(hash)
	if err != nil {
		return false, fmt.Errorf("parse blob hash %q: %w", hash, err)
	}
	index, err := r.loadedIndex(context.Background(), false)
	if err != nil {
		return false, err
	}
	_, ok := index[id]
	return ok, nil
}

// Refresh forces the next resolution to reconcile against the current
// index-file listing (new files folded in, vanished files dropped —
// future prune merges index files, so the set is not append-only forever).
func (r *ObjectReader) Refresh() error {
	_, err := r.loadedIndex(context.Background(), true)
	return err
}

// OpenBlob returns a verified stream of the blob's raw content and its
// size, retrying exactly once through an index reload on any miss —
// covering both a stale index and a pack retired by a future prune.
func (r *ObjectReader) OpenBlob(ctx context.Context, hash string) (io.ReadCloser, int64, error) {
	id, err := pack.ParseBlobID(hash)
	if err != nil {
		return nil, 0, fmt.Errorf("parse blob hash %q: %w", hash, err)
	}
	index, err := r.loadedIndex(ctx, false)
	if err != nil {
		return nil, 0, err
	}
	stream, size, err := r.openIndexed(ctx, index, id)
	if err == nil {
		return stream, size, nil
	}
	if !errors.Is(err, ErrBlobNotFound) && !errors.Is(err, objstore.ErrNotExist) {
		return nil, 0, err
	}
	// One bounded re-resolution: reload the index union and retry.
	if index, err = r.loadedIndex(ctx, true); err != nil {
		return nil, 0, err
	}
	return r.openIndexed(ctx, index, id)
}

func (r *ObjectReader) openIndexed(ctx context.Context, index map[pack.BlobID]backup.IndexEntry,
	id pack.BlobID) (io.ReadCloser, int64, error) {
	indexed, ok := index[id]
	if !ok {
		return nil, 0, fmt.Errorf("%w: %s", ErrBlobNotFound, id)
	}
	packKey := "packs/" + indexed.PackID[:2] + "/" + indexed.PackID + packExt
	size, err := r.packSize(ctx, packKey)
	if err != nil {
		return nil, 0, err
	}
	reader, err := pack.NewReaderFromReaderAt(
		&objstore.ReaderAt{Ctx: ctx, Store: r.store, Key: packKey, ObjectSize: size},
		size, indexed.PackID, nil, pack.ReaderOptions{})
	if err != nil {
		return nil, 0, fmt.Errorf("open pack %s: %w", indexed.PackID, err)
	}
	// Match kit's Repo.OpenBlob exactly: the pack footer is authoritative,
	// and any disagreement with the index record fails closed.
	var authoritative *pack.Entry
	for _, entry := range reader.Entries() {
		if entry.ID == id {
			e := entry
			authoritative = &e
			break
		}
	}
	if authoritative == nil {
		return nil, 0, errors.Join(
			fmt.Errorf("blob %s not found in pack %s (index inconsistency)", id, indexed.PackID),
			reader.Close())
	}
	if authoritative.Offset != indexed.Offset || authoritative.StoredLen != indexed.StoredLen ||
		authoritative.Flags != indexed.Flags {
		return nil, 0, errors.Join(
			fmt.Errorf("blob %s index metadata disagrees with pack %s footer", id, indexed.PackID),
			reader.Close())
	}
	blob, err := reader.OpenBlob(ctx, *authoritative)
	if err != nil {
		return nil, 0, errors.Join(err, reader.Close())
	}
	return &objectBlobStream{blob: blob, reader: reader},
		int64(authoritative.RawLen), nil //nolint:gosec // format-v1 raw lengths fit int64
}

// objectBlobStream pairs the blob stream with its pack reader so Close
// releases both, mirroring kit's BlobStream.
type objectBlobStream struct {
	blob     *pack.BlobReader
	reader   *pack.Reader
	closed   bool
	closeErr error
}

func (s *objectBlobStream) Read(p []byte) (int, error) { return s.blob.Read(p) }

func (s *objectBlobStream) Close() error {
	if s.closed {
		return s.closeErr
	}
	s.closed = true
	s.closeErr = errors.Join(s.blob.Close(), s.reader.Close())
	return s.closeErr
}

func (r *ObjectReader) packSize(ctx context.Context, key string) (int64, error) {
	r.mu.Lock()
	if size, ok := r.packSizes[key]; ok {
		r.mu.Unlock()
		return size, nil
	}
	r.mu.Unlock()
	size, err := r.store.Size(ctx, key)
	if err != nil {
		return 0, fmt.Errorf("size pack %s: %w", key, err)
	}
	r.mu.Lock()
	r.packSizes[key] = size
	r.mu.Unlock()
	return size, nil
}

// loadedIndex returns the index union, folding in index files listed under
// indexes/. Individual files are immutable and cached by name; a reload
// re-lists and reconciles (adds new files, drops vanished ones) without
// refetching cached content.
func (r *ObjectReader) loadedIndex(ctx context.Context, reload bool) (map[pack.BlobID]backup.IndexEntry, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.index != nil && !reload {
		return r.index, nil
	}
	keys, err := r.store.List(ctx, "indexes/")
	if err != nil {
		return nil, fmt.Errorf("list index files: %w", err)
	}
	current := make(map[string]struct{}, len(keys))
	sort.Strings(keys)
	for _, key := range keys {
		if !strings.HasSuffix(key, ".mvidx") {
			continue
		}
		current[key] = struct{}{}
		if _, cached := r.perFile[key]; cached {
			continue
		}
		data, err := r.store.ReadAll(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("read index %s: %w", key, err)
		}
		entries, err := backup.DecodeIndex(data)
		if err != nil {
			return nil, fmt.Errorf("decode index %s: %w", key, err)
		}
		r.perFile[key] = entries
	}
	for key := range r.perFile {
		if _, ok := current[key]; !ok {
			delete(r.perFile, key) // merged away by a future prune
		}
	}
	index := make(map[pack.BlobID]backup.IndexEntry)
	for _, key := range sortedKeys(r.perFile) {
		for _, entry := range r.perFile[key] {
			index[entry.Blob] = entry
		}
	}
	r.index = index
	return index, nil
}

func sortedKeys(m map[string][]backup.IndexEntry) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Close drops cached state; the object store holds no descriptors.
func (r *ObjectReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.index = nil
	r.perFile = map[string][]backup.IndexEntry{}
	return nil
}
