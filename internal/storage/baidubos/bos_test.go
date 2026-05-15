package baidubos

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/baidu/baiducloud-sdk-go/bce"
	"github.com/baidu/baiducloud-sdk-go/bos"
	oss "github.com/langgenius/dify-cloud-kit/oss"
)

type mockBOSClient struct {
	objects  map[string][]byte
	err      error
	listFn   func(req bos.ListObjectsRequest, option *bce.SignOption) (*bos.ListObjectsResponse, error)
}

func newMockBOSClient() *mockBOSClient {
	return &mockBOSClient{
		objects: make(map[string][]byte),
	}
}

func (m *mockBOSClient) PutObject(bucketName string, objectKey string, data interface{}, metadata *bos.ObjectMetadata, option *bce.SignOption) (bos.PutObjectResponse, error) {
	if m.err != nil {
		return nil, m.err
	}
	switch v := data.(type) {
	case []byte:
		m.objects[bucketName+"/"+objectKey] = v
	case string:
		m.objects[bucketName+"/"+objectKey] = []byte(v)
	case io.Reader:
		var buf bytes.Buffer
		buf.ReadFrom(v)
		m.objects[bucketName+"/"+objectKey] = buf.Bytes()
	}
	return bos.PutObjectResponse{}, nil
}

func (m *mockBOSClient) GetObject(bucketName string, objectKey string, option *bce.SignOption) (*bos.Object, error) {
	if m.err != nil {
		return nil, m.err
	}
	data, ok := m.objects[bucketName+"/"+objectKey]
	if !ok {
		return nil, &bce.Error{StatusCode: http.StatusNotFound, Code: "NoSuchKey", Message: "object not found"}
	}
	return &bos.Object{
		ObjectMetadata: &bos.ObjectMetadata{ContentLength: int64(len(data))},
		ObjectContent:  io.NopCloser(bytes.NewReader(data)),
	}, nil
}

func (m *mockBOSClient) GetObjectMetadata(bucketName string, objectKey string, option *bce.SignOption) (*bos.ObjectMetadata, error) {
	if m.err != nil {
		return nil, m.err
	}
	data, ok := m.objects[bucketName+"/"+objectKey]
	if !ok {
		return nil, &bce.Error{StatusCode: http.StatusNotFound, Code: "NoSuchKey", Message: "object not found"}
	}
	return &bos.ObjectMetadata{ContentLength: int64(len(data))}, nil
}

func (m *mockBOSClient) ListObjectsFromRequest(req bos.ListObjectsRequest, option *bce.SignOption) (*bos.ListObjectsResponse, error) {
	if m.err != nil {
		return nil, m.err
	}
	if m.listFn != nil {
		return m.listFn(req, option)
	}
	prefix := req.Prefix
	var contents []bos.ObjectSummary
	for key := range m.objects {
		fullKey := strings.TrimPrefix(key, req.BucketName+"/")
		if strings.HasPrefix(fullKey, prefix) {
			contents = append(contents, bos.ObjectSummary{Key: fullKey})
		}
	}
	return &bos.ListObjectsResponse{
		Contents:    contents,
		IsTruncated: false,
	}, nil
}

func (m *mockBOSClient) DeleteObject(bucketName string, objectKey string, option *bce.SignOption) error {
	if m.err != nil {
		return m.err
	}
	delete(m.objects, bucketName+"/"+objectKey)
	return nil
}

func newTestStorage(client *mockBOSClient) *BaiduBOSStorage {
	return newBaiduBOSStorageWithClient("test-bucket", client)
}

func TestNewBaiduBOSStorage_MissingBucket(t *testing.T) {
	_, err := NewBaiduBOSStorage(BaiduBOSConfig{
		AccessKey: "ak",
		SecretKey: "sk",
		Region:    "bj",
	})
	if err == nil {
		t.Fatal("expected error for missing bucket")
	}
}

func TestNewBaiduBOSStorage_MissingCredentials(t *testing.T) {
	_, err := NewBaiduBOSStorage(BaiduBOSConfig{
		Bucket: "my-bucket",
		Region: "bj",
	})
	if err == nil {
		t.Fatal("expected error for missing credentials")
	}
}

func TestNewBaiduBOSStorage_MissingEndpointAndRegion(t *testing.T) {
	_, err := NewBaiduBOSStorage(BaiduBOSConfig{
		Bucket:    "my-bucket",
		AccessKey: "ak",
		SecretKey: "sk",
	})
	if err == nil {
		t.Fatal("expected error for missing endpoint and region")
	}
}

func TestNewBaiduBOSStorage_TrimsWhitespace(t *testing.T) {
	storage, err := NewBaiduBOSStorage(BaiduBOSConfig{
		Bucket:    "  my-bucket  ",
		AccessKey: "  ak  ",
		SecretKey: "  sk  ",
		Region:    "  bj  ",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if storage.Type() != OSS_TYPE_BAIDU_BOS {
		t.Fatalf("expected type %s, got %s", OSS_TYPE_BAIDU_BOS, storage.Type())
	}
}

func TestSaveAndLoad(t *testing.T) {
	mock := newMockBOSClient()
	storage := newTestStorage(mock)

	data := []byte("hello world")
	err := storage.Save("test-key", data)
	if err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	loaded, err := storage.Load("test-key")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if !bytes.Equal(loaded, data) {
		t.Fatalf("expected %s, got %s", data, loaded)
	}
}

func TestLoad_NotFound(t *testing.T) {
	mock := newMockBOSClient()
	storage := newTestStorage(mock)

	_, err := storage.Load("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent key")
	}
}

func TestSave_Error(t *testing.T) {
	mock := newMockBOSClient()
	mock.err = errors.New("network error")
	storage := newTestStorage(mock)

	err := storage.Save("key", []byte("data"))
	if err == nil {
		t.Fatal("expected error from Save")
	}
}

func TestExists_True(t *testing.T) {
	mock := newMockBOSClient()
	storage := newTestStorage(mock)
	mock.objects["test-bucket/key"] = []byte("data")

	exists, err := storage.Exists("key")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !exists {
		t.Fatal("expected exists=true")
	}
}

func TestExists_False(t *testing.T) {
	mock := newMockBOSClient()
	storage := newTestStorage(mock)

	exists, err := storage.Exists("nonexistent")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if exists {
		t.Fatal("expected exists=false")
	}
}

func TestExists_OtherError(t *testing.T) {
	mock := newMockBOSClient()
	mock.err = &bce.Error{StatusCode: http.StatusForbidden, Code: "AccessDenied", Message: "forbidden"}
	storage := newTestStorage(mock)

	_, err := storage.Exists("key")
	if err == nil {
		t.Fatal("expected error for non-404 error")
	}
}

func TestState(t *testing.T) {
	mock := newMockBOSClient()
	storage := newTestStorage(mock)
	mock.objects["test-bucket/key"] = []byte("hello")

	state, err := storage.State("key")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state.Size != 5 {
		t.Fatalf("expected size 5, got %d", state.Size)
	}
}

func TestState_NotFound(t *testing.T) {
	mock := newMockBOSClient()
	storage := newTestStorage(mock)

	_, err := storage.State("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent key")
	}
}

func TestList(t *testing.T) {
	mock := newMockBOSClient()
	storage := newTestStorage(mock)
	mock.objects["test-bucket/plugin/a.difypkg"] = []byte("a")
	mock.objects["test-bucket/plugin/b.difypkg"] = []byte("b")
	mock.objects["test-bucket/plugin/c/d.difypkg"] = []byte("d")

	paths, err := storage.List("plugin")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(paths) != 3 {
		t.Fatalf("expected 3 paths, got %d", len(paths))
	}
	for _, p := range paths {
		if p.IsDir {
			t.Fatalf("expected IsDir=false for %s", p.Path)
		}
	}
}

func TestList_Empty(t *testing.T) {
	mock := newMockBOSClient()
	storage := newTestStorage(mock)

	paths, err := storage.List("plugin")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(paths) != 0 {
		t.Fatalf("expected 0 paths, got %d", len(paths))
	}
}

func TestList_AddsTrailingSlash(t *testing.T) {
	mock := newMockBOSClient()
	storage := newTestStorage(mock)
	mock.objects["test-bucket/assets/icon.svg"] = []byte("svg")

	paths, err := storage.List("assets")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(paths) != 1 || paths[0].Path != "icon.svg" {
		t.Fatalf("expected [icon.svg], got %v", paths)
	}
}

func TestList_PaginationWithNextMarker(t *testing.T) {
	mock := newMockBOSClient()
	callCount := 0
	mock.listFn = func(req bos.ListObjectsRequest, option *bce.SignOption) (*bos.ListObjectsResponse, error) {
		callCount++
		if callCount == 1 {
			return &bos.ListObjectsResponse{
				Contents:    []bos.ObjectSummary{{Key: "plugin/a.difypkg"}, {Key: "plugin/b.difypkg"}},
				IsTruncated: true,
				NextMarker:  "plugin/b.difypkg",
			}, nil
		}
		return &bos.ListObjectsResponse{
			Contents:    []bos.ObjectSummary{{Key: "plugin/c.difypkg"}},
			IsTruncated: false,
		}, nil
	}
	storage := newTestStorage(mock)

	paths, err := storage.List("plugin")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(paths) != 3 {
		t.Fatalf("expected 3 paths, got %d", len(paths))
	}
	if callCount != 2 {
		t.Fatalf("expected 2 API calls, got %d", callCount)
	}
}

func TestList_PaginationFallbackToLastKey(t *testing.T) {
	mock := newMockBOSClient()
	callCount := 0
	mock.listFn = func(req bos.ListObjectsRequest, option *bce.SignOption) (*bos.ListObjectsResponse, error) {
		callCount++
		if req.Marker == "" {
			return &bos.ListObjectsResponse{
				Contents:    []bos.ObjectSummary{{Key: "plugin/a.difypkg"}, {Key: "plugin/b.difypkg"}},
				IsTruncated: true,
				NextMarker:  "",
			}, nil
		}
		if req.Marker == "plugin/b.difypkg" {
			return &bos.ListObjectsResponse{
				Contents:    []bos.ObjectSummary{{Key: "plugin/c.difypkg"}},
				IsTruncated: false,
			}, nil
		}
		t.Fatalf("unexpected marker: %s", req.Marker)
		return nil, nil
	}
	storage := newTestStorage(mock)

	paths, err := storage.List("plugin")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(paths) != 3 {
		t.Fatalf("expected 3 paths, got %d", len(paths))
	}
	if callCount != 2 {
		t.Fatalf("expected 2 API calls, got %d", callCount)
	}
}

func TestList_PaginationTruncatedButEmptyContents(t *testing.T) {
	mock := newMockBOSClient()
	mock.listFn = func(req bos.ListObjectsRequest, option *bce.SignOption) (*bos.ListObjectsResponse, error) {
		return &bos.ListObjectsResponse{
			Contents:    nil,
			IsTruncated: true,
			NextMarker:  "",
		}, nil
	}
	storage := newTestStorage(mock)

	paths, err := storage.List("plugin")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(paths) != 0 {
		t.Fatalf("expected 0 paths, got %d", len(paths))
	}
}

func TestList_Error(t *testing.T) {
	mock := newMockBOSClient()
	mock.err = errors.New("network error")
	storage := newTestStorage(mock)

	_, err := storage.List("plugin")
	if err == nil {
		t.Fatal("expected error from List")
	}
}

func TestDelete(t *testing.T) {
	mock := newMockBOSClient()
	storage := newTestStorage(mock)
	mock.objects["test-bucket/key"] = []byte("data")

	err := storage.Delete("key")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := mock.objects["test-bucket/key"]; ok {
		t.Fatal("expected object to be deleted")
	}
}

func TestDelete_Error(t *testing.T) {
	mock := newMockBOSClient()
	mock.err = errors.New("network error")
	storage := newTestStorage(mock)

	err := storage.Delete("key")
	if err == nil {
		t.Fatal("expected error from Delete")
	}
}

func TestType(t *testing.T) {
	mock := newMockBOSClient()
	storage := newTestStorage(mock)

	if storage.Type() != "baidu_bos" {
		t.Fatalf("expected baidu_bos, got %s", storage.Type())
	}
}

func TestIsNotFoundError_BceError404(t *testing.T) {
	err := &bce.Error{StatusCode: http.StatusNotFound, Code: "NoSuchKey", Message: "not found"}
	if !isNotFoundError(err) {
		t.Fatal("expected isNotFoundError=true for 404 BceError")
	}
}

func TestIsNotFoundError_BceError403(t *testing.T) {
	err := &bce.Error{StatusCode: http.StatusForbidden, Code: "AccessDenied", Message: "forbidden"}
	if isNotFoundError(err) {
		t.Fatal("expected isNotFoundError=false for 403 BceError")
	}
}

func TestIsNotFoundError_OtherError(t *testing.T) {
	err := errors.New("some error")
	if isNotFoundError(err) {
		t.Fatal("expected isNotFoundError=false for non-BceError")
	}
}

func TestBaiduBOSStorage_ImplementsOSSInterface(t *testing.T) {
	var _ oss.OSS = (*BaiduBOSStorage)(nil)
}
