package objstore

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// emptyPayloadSHA256 is the SHA-256 of the empty string: the payload hash
// for every request this read-only client sends (GET/HEAD carry no body).
const emptyPayloadSHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// S3Config describes an S3-compatible target. Credentials always come from
// the standard environment (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
// optional AWS_SESSION_TOKEN) — never from msgvault config files, which
// are rewritten by config Save.
type S3Config struct {
	Bucket string
	Prefix string // key prefix inside the bucket, no leading slash
	Region string // default us-east-1; R2 uses "auto"
	// Endpoint overrides the AWS default (B2, R2, MinIO), e.g.
	// https://s3.us-west-000.backblazeb2.com — requests are path-style.
	Endpoint string
}

// S3Store is a read-only SigV4 client speaking the minimal S3 surface the
// repository reader needs: ranged GET, HEAD, and ListObjectsV2.
type S3Store struct {
	cfg    S3Config
	base   *url.URL
	client *http.Client
	creds  s3Credentials
	now    func() time.Time // test seam
}

type s3Credentials struct {
	accessKey    string
	secretKey    string
	sessionToken string
}

// ParseS3URL splits s3://bucket/prefix into bucket and prefix.
func ParseS3URL(raw string) (bucket, prefix string, err error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "s3" || u.Host == "" {
		return "", "", fmt.Errorf("objstore: invalid s3 URL %q (want s3://bucket/prefix)", raw)
	}
	return u.Host, strings.Trim(u.Path, "/"), nil
}

// NewS3Store builds the client. Credentials are read from the environment
// once, here, so a missing key fails at configuration time rather than on
// the first blob read.
func NewS3Store(cfg S3Config, client *http.Client) (*S3Store, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("objstore: s3 bucket is required")
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	endpoint := cfg.Endpoint
	if endpoint == "" {
		endpoint = "https://s3." + cfg.Region + ".amazonaws.com"
	}
	base, err := url.Parse(endpoint)
	if err != nil || (base.Scheme != "https" && base.Scheme != "http") || base.Host == "" {
		return nil, fmt.Errorf("objstore: invalid s3 endpoint %q", cfg.Endpoint)
	}
	creds := s3Credentials{
		accessKey:    os.Getenv("AWS_ACCESS_KEY_ID"),
		secretKey:    os.Getenv("AWS_SECRET_ACCESS_KEY"),
		sessionToken: os.Getenv("AWS_SESSION_TOKEN"),
	}
	if creds.accessKey == "" || creds.secretKey == "" {
		return nil, errors.New(
			"objstore: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set for an s3 offload repository")
	}
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Minute}
	}
	return &S3Store{cfg: cfg, base: base, client: client, creds: creds, now: time.Now}, nil
}

func (s *S3Store) objectKey(key string) string {
	if s.cfg.Prefix == "" {
		return key
	}
	return s.cfg.Prefix + "/" + key
}

// request builds and signs a request for the bucket. objectKey empty means
// a bucket-level request (listing).
func (s *S3Store) request(ctx context.Context, method, objectKey string, query url.Values, header http.Header) (*http.Request, error) {
	u := *s.base
	segments := []string{s.cfg.Bucket}
	if objectKey != "" {
		segments = append(segments, strings.Split(objectKey, "/")...)
	}
	u.Path = strings.TrimSuffix(u.Path, "/") + "/" + strings.Join(segments, "/")
	u.RawQuery = canonicalQuery(query)
	req, err := http.NewRequestWithContext(ctx, method, u.String(), nil)
	if err != nil {
		return nil, err
	}
	for k, vs := range header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	s.sign(req)
	return req, nil
}

// sign applies AWS Signature Version 4 for a bodyless request.
func (s *S3Store) sign(req *http.Request) {
	now := s.now().UTC()
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")

	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", emptyPayloadSHA256)
	if s.creds.sessionToken != "" {
		req.Header.Set("X-Amz-Security-Token", s.creds.sessionToken)
	}

	signedHeaderNames := []string{"host", "x-amz-content-sha256", "x-amz-date"}
	if s.creds.sessionToken != "" {
		signedHeaderNames = append(signedHeaderNames, "x-amz-security-token")
	}
	if req.Header.Get("Range") != "" {
		signedHeaderNames = append(signedHeaderNames, "range")
	}
	sort.Strings(signedHeaderNames)

	var canonicalHeaders strings.Builder
	for _, name := range signedHeaderNames {
		value := req.Header.Get(name)
		if name == "host" {
			value = req.Host
			if value == "" {
				value = req.URL.Host
			}
		}
		canonicalHeaders.WriteString(name + ":" + strings.TrimSpace(value) + "\n")
	}
	signedHeaders := strings.Join(signedHeaderNames, ";")

	canonicalRequest := strings.Join([]string{
		req.Method,
		canonicalURI(req.URL),
		req.URL.RawQuery,
		canonicalHeaders.String(),
		signedHeaders,
		emptyPayloadSHA256,
	}, "\n")

	scope := strings.Join([]string{dateStamp, s.cfg.Region, "s3", "aws4_request"}, "/")
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		scope,
		hexSHA256([]byte(canonicalRequest)),
	}, "\n")

	key := hmacSHA256([]byte("AWS4"+s.creds.secretKey), dateStamp)
	key = hmacSHA256(key, s.cfg.Region)
	key = hmacSHA256(key, "s3")
	key = hmacSHA256(key, "aws4_request")
	signature := hex.EncodeToString(hmacSHA256(key, stringToSign))

	req.Header.Set("Authorization", fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		s.creds.accessKey, scope, signedHeaders, signature))
}

// canonicalURI RFC3986-encodes each path segment, preserving slashes.
func canonicalURI(u *url.URL) string {
	segments := strings.Split(u.EscapedPath(), "/")
	for i, seg := range segments {
		decoded, err := url.PathUnescape(seg)
		if err != nil {
			decoded = seg
		}
		segments[i] = awsURIEscape(decoded)
	}
	return strings.Join(segments, "/")
}

// awsURIEscape implements the AWS canonical percent-encoding: unreserved
// characters (A-Za-z0-9, '-', '.', '_', '~') stay literal, everything else
// is %XX with uppercase hex.
func awsURIEscape(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9',
			c == '-', c == '.', c == '_', c == '~':
			b.WriteByte(c)
		default:
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

func canonicalQuery(query url.Values) string {
	if len(query) == 0 {
		return ""
	}
	keys := make([]string, 0, len(query))
	for k := range query {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		values := append([]string(nil), query[k]...)
		sort.Strings(values)
		for _, v := range values {
			parts = append(parts, awsURIEscape(k)+"="+awsURIEscape(v))
		}
	}
	return strings.Join(parts, "&")
}

func hexSHA256(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func hmacSHA256(key []byte, data string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(data))
	return mac.Sum(nil)
}

func (s *S3Store) do(req *http.Request, wantStatus ...int) (*http.Response, error) {
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("objstore: s3 request: %w", err)
	}
	for _, want := range wantStatus {
		if resp.StatusCode == want {
			return resp, nil
		}
	}
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("objstore: %s: %w", req.URL.Path, ErrNotExist)
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
	return nil, fmt.Errorf("objstore: s3 %s %s: status %d: %s",
		req.Method, req.URL.Path, resp.StatusCode, strings.TrimSpace(string(body)))
}

func (s *S3Store) ReadRange(ctx context.Context, key string, off, length int64) (io.ReadCloser, error) {
	if length <= 0 {
		return io.NopCloser(strings.NewReader("")), nil
	}
	header := http.Header{}
	header.Set("Range", fmt.Sprintf("bytes=%d-%d", off, off+length-1))
	req, err := s.request(ctx, http.MethodGet, s.objectKey(key), nil, header)
	if err != nil {
		return nil, err
	}
	resp, err := s.do(req, http.StatusPartialContent, http.StatusOK)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusOK {
		// The server ignored Range (rare; some proxies). Serve the window
		// by discarding and truncating so callers still get exact bytes.
		if _, err := io.CopyN(io.Discard, resp.Body, off); err != nil {
			_ = resp.Body.Close()
			return nil, fmt.Errorf("objstore: seeking within un-ranged response: %w", err)
		}
		return newLimitedBody(resp.Body, length), nil
	}
	return resp.Body, nil
}

type limitedBody struct {
	io.Reader
	c io.Closer
}

func newLimitedBody(rc io.ReadCloser, n int64) io.ReadCloser {
	return &limitedBody{Reader: io.LimitReader(rc, n), c: rc}
}

func (l *limitedBody) Close() error { return l.c.Close() }

func (s *S3Store) ReadAll(ctx context.Context, key string) ([]byte, error) {
	req, err := s.request(ctx, http.MethodGet, s.objectKey(key), nil, nil)
	if err != nil {
		return nil, err
	}
	resp, err := s.do(req, http.StatusOK)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close() //nolint:errcheck
	return io.ReadAll(resp.Body)
}

func (s *S3Store) Size(ctx context.Context, key string) (int64, error) {
	req, err := s.request(ctx, http.MethodHead, s.objectKey(key), nil, nil)
	if err != nil {
		return 0, err
	}
	resp, err := s.do(req, http.StatusOK)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close() //nolint:errcheck
	size, err := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("objstore: s3 HEAD %s: bad Content-Length: %w", key, err)
	}
	return size, nil
}

type listBucketResult struct {
	Contents []struct {
		Key string `xml:"Key"`
	} `xml:"Contents"`
	IsTruncated           bool   `xml:"IsTruncated"`
	NextContinuationToken string `xml:"NextContinuationToken"`
}

func (s *S3Store) List(ctx context.Context, prefix string) ([]string, error) {
	fullPrefix := s.objectKey(prefix)
	var keys []string
	token := ""
	for {
		query := url.Values{"list-type": {"2"}, "prefix": {fullPrefix}}
		if token != "" {
			query.Set("continuation-token", token)
		}
		req, err := s.request(ctx, http.MethodGet, "", query, nil)
		if err != nil {
			return nil, err
		}
		resp, err := s.do(req, http.StatusOK)
		if err != nil {
			return nil, err
		}
		var page listBucketResult
		decodeErr := xml.NewDecoder(resp.Body).Decode(&page)
		closeErr := resp.Body.Close()
		if err := errors.Join(decodeErr, closeErr); err != nil {
			return nil, fmt.Errorf("objstore: s3 list %s: %w", prefix, err)
		}
		for _, obj := range page.Contents {
			key := strings.TrimPrefix(obj.Key, s.cfg.Prefix)
			keys = append(keys, strings.TrimPrefix(key, "/"))
		}
		if !page.IsTruncated || page.NextContinuationToken == "" {
			return keys, nil
		}
		token = page.NextContinuationToken
	}
}
