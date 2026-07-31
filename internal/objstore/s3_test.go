package objstore

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	fakeAccessKey = "AKIDFAKEACCESSKEY"
	fakeSecretKey = "fake/secret/key+value"
	fakeBucket    = "vault-bucket"
	fakePrefix    = "msgvault/repo"
)

// fakeS3 is an httptest S3 endpoint that verifies each request's SigV4
// signature the way a real server does — recomputing it from the received
// request and the shared secret — before serving path-style GET/HEAD/List.
type fakeS3 struct {
	t       *testing.T
	store   *S3Store // signing reference for verification
	objects map[string]string
	pageMax int
}

func (f *fakeS3) verify(r *http.Request) bool {
	auth := r.Header.Get("Authorization")
	if auth == "" || r.Header.Get("X-Amz-Content-Sha256") != emptyPayloadSHA256 {
		return false
	}
	// Recompute the signature over the request as received.
	clone := r.Clone(context.Background())
	clone.Host = r.Host
	clone.Header.Del("Authorization")
	f.store.sign(clone)
	return clone.Header.Get("Authorization") == auth
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !f.verify(r) {
		w.WriteHeader(http.StatusForbidden)
		fmt.Fprint(w, "<Error><Code>SignatureDoesNotMatch</Code></Error>")
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/"+fakeBucket)
	path = strings.TrimPrefix(path, "/")

	if path == "" { // bucket-level: ListObjectsV2
		f.serveList(w, r)
		return
	}
	content, ok := f.objects[path]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, "<Error><Code>NoSuchKey</Code></Error>")
		return
	}
	if r.Method == http.MethodHead {
		w.Header().Set("Content-Length", strconv.Itoa(len(content)))
		w.WriteHeader(http.StatusOK)
		return
	}
	if rng := r.Header.Get("Range"); rng != "" {
		var start, end int
		_, err := fmt.Sscanf(rng, "bytes=%d-%d", &start, &end)
		require.NoError(f.t, err)
		require.Less(f.t, end, len(content), "client must never request past the end")
		w.Header().Set("Content-Range",
			fmt.Sprintf("bytes %d-%d/%d", start, end, len(content)))
		w.WriteHeader(http.StatusPartialContent)
		fmt.Fprint(w, content[start:end+1])
		return
	}
	fmt.Fprint(w, content)
}

func (f *fakeS3) serveList(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	require.Equal(f.t, "2", q.Get("list-type"))
	prefix := q.Get("prefix")
	var keys []string
	for k := range f.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	start := 0
	if tok := q.Get("continuation-token"); tok != "" {
		start, _ = strconv.Atoi(tok)
	}
	end := min(start+f.pageMax, len(keys))
	type object struct {
		Key string `xml:"Key"`
	}
	page := struct {
		XMLName               xml.Name `xml:"ListBucketResult"`
		Contents              []object
		IsTruncated           bool
		NextContinuationToken string `xml:",omitempty"`
	}{IsTruncated: end < len(keys)}
	for _, k := range keys[start:end] {
		page.Contents = append(page.Contents, object{Key: k})
	}
	if page.IsTruncated {
		page.NextContinuationToken = strconv.Itoa(end)
	}
	require.NoError(f.t, xml.NewEncoder(w).Encode(page))
}

func newFakeS3Store(t *testing.T) (*S3Store, *fakeS3) {
	t.Helper()
	t.Setenv("AWS_ACCESS_KEY_ID", fakeAccessKey)
	t.Setenv("AWS_SECRET_ACCESS_KEY", fakeSecretKey)
	t.Setenv("AWS_SESSION_TOKEN", "")

	fake := &fakeS3{t: t, objects: map[string]string{}, pageMax: 2}
	for key, content := range contractObjects {
		fake.objects[fakePrefix+"/"+key] = content
	}
	server := httptest.NewServer(fake)
	t.Cleanup(server.Close)

	store, err := NewS3Store(S3Config{
		Bucket: fakeBucket, Prefix: fakePrefix,
		Region: "us-east-1", Endpoint: server.URL,
	}, server.Client())
	require.NoError(t, err)
	fake.store = store
	return store, fake
}

func TestS3StoreContract(t *testing.T) {
	store, _ := newFakeS3Store(t)
	runStoreContract(t, store)
}

func TestS3ListPaginates(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	store, fake := newFakeS3Store(t)
	// Force pagination across everything (5 objects, pages of 2).
	fake.pageMax = 2
	keys, err := store.List(context.Background(), "")
	require.NoError(err)
	sort.Strings(keys)
	want := make([]string, 0, len(contractObjects))
	for k := range contractObjects {
		want = append(want, k)
	}
	sort.Strings(want)
	assert.Equal(want, keys)
}

func TestS3RejectsBadCredentials(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	store, fake := newFakeS3Store(t)
	// Corrupt the client's secret after the fake captured the good signer:
	// the server must now refuse, and the client must surface the failure.
	bad := *store
	bad.creds.secretKey = "wrong"
	_, err := bad.ReadAll(context.Background(), "config.toml")
	require.Error(err)
	assert.Contains(err.Error(), "403")
	_ = fake
}

func TestNewS3StoreRequiresCredentials(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	_, err := NewS3Store(S3Config{Bucket: "b"}, nil)
	require.Error(err)
	assert.Contains(err.Error(), "AWS_ACCESS_KEY_ID")
}

func TestParseS3URL(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	bucket, prefix, err := ParseS3URL("s3://my-bucket/some/prefix/")
	require.NoError(err)
	assert.Equal("my-bucket", bucket)
	assert.Equal("some/prefix", prefix)

	bucket, prefix, err = ParseS3URL("s3://only-bucket")
	require.NoError(err)
	assert.Equal("only-bucket", bucket)
	assert.Empty(prefix)

	for _, bad := range []string{"s3://", "http://bucket/x", "not-a-url"} {
		_, _, err := ParseS3URL(bad)
		assert.Error(err, bad)
	}
}

func TestS3SessionTokenIsSigned(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	t.Setenv("AWS_ACCESS_KEY_ID", fakeAccessKey)
	t.Setenv("AWS_SECRET_ACCESS_KEY", fakeSecretKey)
	t.Setenv("AWS_SESSION_TOKEN", "session-token-value")

	var sawToken, sawSigned bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawToken = r.Header.Get("X-Amz-Security-Token") == "session-token-value"
		sawSigned = strings.Contains(r.Header.Get("Authorization"), "x-amz-security-token")
		fmt.Fprint(w, "ok")
	}))
	t.Cleanup(server.Close)

	store, err := NewS3Store(S3Config{Bucket: "b", Endpoint: server.URL}, server.Client())
	require.NoError(err)
	_, err = store.ReadAll(context.Background(), "k")
	require.NoError(err)
	assert.True(sawToken, "session token header sent")
	assert.True(sawSigned, "session token header included in SignedHeaders")
}
