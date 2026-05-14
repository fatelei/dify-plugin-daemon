package baidubos

import (
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/baidu/baiducloud-sdk-go/bce"
	"github.com/baidu/baiducloud-sdk-go/bos"
	oss "github.com/langgenius/dify-cloud-kit/oss"
)

const OSS_TYPE_BAIDU_BOS = "baidu_bos"

type bosClient interface {
	PutObject(bucketName string, objectKey string, data interface{}, metadata *bos.ObjectMetadata, option *bce.SignOption) (bos.PutObjectResponse, error)
	GetObject(bucketName string, objectKey string, option *bce.SignOption) (*bos.Object, error)
	GetObjectMetadata(bucketName string, objectKey string, option *bce.SignOption) (*bos.ObjectMetadata, error)
	ListObjectsFromRequest(req bos.ListObjectsRequest, option *bce.SignOption) (*bos.ListObjectsResponse, error)
	DeleteObject(bucketName string, objectKey string, option *bce.SignOption) error
}

type BaiduBOSStorage struct {
	bucket string
	client bosClient
}

type BaiduBOSConfig struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	Region    string
	Bucket    string
}

func (c *BaiduBOSConfig) Validate() error {
	if c.Bucket == "" || c.AccessKey == "" || c.SecretKey == "" {
		return fmt.Errorf("bucket, accessKey, secretKey cannot be empty")
	}
	if c.Endpoint == "" && c.Region == "" {
		return fmt.Errorf("endpoint or region must be provided")
	}
	return nil
}

func NewBaiduBOSStorage(config BaiduBOSConfig) (oss.OSS, error) {
	config.Endpoint = strings.TrimSpace(config.Endpoint)
	config.Region = strings.TrimSpace(config.Region)
	config.AccessKey = strings.TrimSpace(config.AccessKey)
	config.SecretKey = strings.TrimSpace(config.SecretKey)
	config.Bucket = strings.TrimSpace(config.Bucket)

	if err := config.Validate(); err != nil {
		return nil, oss.ErrArgumentInvalid.WithDetail(err.Error())
	}

	credentials := bce.NewCredentials(config.AccessKey, config.SecretKey)
	bceConfig := bce.NewConfig(credentials)

	if config.Endpoint != "" {
		bceConfig.Endpoint = config.Endpoint
	}
	if config.Region != "" {
		bceConfig.Region = config.Region
	}

	client := bos.NewClient(bceConfig)

	return newBaiduBOSStorageWithClient(config.Bucket, client), nil
}

func newBaiduBOSStorageWithClient(bucket string, client bosClient) *BaiduBOSStorage {
	return &BaiduBOSStorage{
		bucket: bucket,
		client: client,
	}
}

func (b *BaiduBOSStorage) Save(key string, data []byte) error {
	_, err := b.client.PutObject(b.bucket, key, data, nil, nil)
	return err
}

func (b *BaiduBOSStorage) Load(key string) ([]byte, error) {
	obj, err := b.client.GetObject(b.bucket, key, nil)
	if err != nil {
		return nil, err
	}
	defer obj.ObjectContent.Close()

	return io.ReadAll(obj.ObjectContent)
}

func (b *BaiduBOSStorage) Exists(key string) (bool, error) {
	_, err := b.client.GetObjectMetadata(b.bucket, key, nil)
	if err != nil {
		if isNotFoundError(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (b *BaiduBOSStorage) State(key string) (oss.OSSState, error) {
	metadata, err := b.client.GetObjectMetadata(b.bucket, key, nil)
	if err != nil {
		return oss.OSSState{}, err
	}

	return oss.OSSState{
		Size: metadata.ContentLength,
	}, nil
}

func (b *BaiduBOSStorage) List(prefix string) ([]oss.OSSPath, error) {
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}

	marker := ""
	paths := []oss.OSSPath{}
	for {
		req := bos.ListObjectsRequest{
			BucketName: b.bucket,
			Prefix:     prefix,
			Marker:     marker,
			MaxKeys:    1000,
		}
		resp, err := b.client.ListObjectsFromRequest(req, nil)
		if err != nil {
			return nil, err
		}

		for _, obj := range resp.Contents {
			key := strings.TrimPrefix(obj.Key, prefix)
			key = strings.TrimPrefix(key, "/")

			if key == "" {
				continue
			}
			paths = append(paths, oss.OSSPath{
				Path:  key,
				IsDir: false,
			})
		}

		if !resp.IsTruncated {
			break
		}

		if resp.NextMarker != "" {
			marker = resp.NextMarker
		} else if len(resp.Contents) > 0 {
			marker = resp.Contents[len(resp.Contents)-1].Key
		} else {
			break
		}
	}

	return paths, nil
}

func (b *BaiduBOSStorage) Delete(key string) error {
	return b.client.DeleteObject(b.bucket, key, nil)
}

func (b *BaiduBOSStorage) Type() string {
	return OSS_TYPE_BAIDU_BOS
}

func isNotFoundError(err error) bool {
	if bceErr, ok := err.(*bce.Error); ok {
		return bceErr.StatusCode == http.StatusNotFound
	}
	return false
}
