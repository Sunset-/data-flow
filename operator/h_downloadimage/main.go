package h_downloadimage

import (
	"dyzs/data-flow/concurrent"
	"dyzs/data-flow/context"
	"dyzs/data-flow/logger"
	"dyzs/data-flow/model/gat1400"
	"dyzs/data-flow/model/gat1400/base"
	"dyzs/data-flow/stream"
	"dyzs/data-flow/util"
	"dyzs/data-flow/util/base64"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"
)

func init() {
	stream.RegistHandler("downloadimage", func() stream.Handler {
		return &ImageDownloader{}
	})
}

type ImageDownloader struct {
	executor *concurrent.Executor
	client   *http.Client

	downloadTypes   map[string]bool
	convertUrlRules string
	imageUrlCovert  func(string) string
}

func (h *ImageDownloader) Init(config interface{}) error {
	capacity := 20
	configCapacity := context.GetInt("downloadimage_capacity")
	if configCapacity > 0 {
		capacity = configCapacity
	}

	h.downloadTypes = make(map[string]bool)
	types := context.GetString("downloadimage_types")
	for _, t := range strings.Split(types, ",") {
		t = strings.TrimSpace(t)
		if t != "" {
			h.downloadTypes[t] = true
		}
	}

	h.convertUrlRules = context.GetString("downloadimage_converturl")

	logger.LOG_WARN("------------------ downloadimage config ------------------")
	logger.LOG_WARN("downloadimage_capacity : " + strconv.Itoa(capacity))
	logger.LOG_WARN("downloadimage_types : " + types)
	logger.LOG_WARN("downloadimage_converturl : " + h.convertUrlRules)
	logger.LOG_WARN("------------------------------------------------------")
	h.executor = concurrent.NewExecutor(capacity)
	h.client = &http.Client{
		Transport: &http.Transport{
			DisableKeepAlives:   false, //false 长链接 true 短连接
			Proxy:               http.ProxyFromEnvironment,
			MaxIdleConns:        capacity * 5, //client对与所有host最大空闲连接数总和
			MaxConnsPerHost:     capacity,
			MaxIdleConnsPerHost: capacity,         //连接池对每个host的最大连接数量,当超出这个范围时，客户端会主动关闭到连接
			IdleConnTimeout:     60 * time.Second, //空闲连接在连接池中的超时时间
		},
		Timeout: 5 * time.Second, //粗粒度 时间计算包括从连接(Dial)到读完response body
	}
	h.InitImageUrlConvert()
	return nil
}

func (h *ImageDownloader) Handle(data interface{}, next func(interface{}) error) error {
	wraps, ok := data.([]*gat1400.Gat1400Wrap)
	if !ok {
		return errors.New(fmt.Sprintf("Handle [imagedeal] 数据格式错误，need []*daghub.StandardModelWrap , get %T", reflect.TypeOf(data)))
	}
	if len(wraps) == 0 {
		return nil
	}
	tasks := make([]func(), 0)
	for _, wrap := range wraps {
		for _, item := range wrap.GetSubImageInfos() {
			//下载图片
			if h.downloadTypes[item.Type] {
				func(img *base.SubImageInfo) {
					tasks = append(tasks, func() {
						err := h.downloadImage(img)
						if err != nil {
							logger.LOG_WARN("图片下载失败：", err)
							logger.LOG_WARN("图片下载失败url：", img.StoragePath)
							//转换图片url
							item.StoragePath = h.convertUrl(item.StoragePath)
						}
					})
				}(item)
			} else {
				//转换图片url
				item.StoragePath = h.convertUrl(item.StoragePath)
			}
		}
	}
	if len(tasks) > 0 {
		err := h.executor.SubmitSyncBatch(tasks)
		if err != nil {
			logger.LOG_ERROR("批量下载图片失败：", err)
		}
	}
	return next(wraps)
}

func (h *ImageDownloader) downloadImage(image *base.SubImageInfo) error {
	url := image.StoragePath
	if url == "" {
		return errors.New("图片路径缺失")
	}
	if strings.Index(url, "http://") != 0 && strings.Index(url, "https://") != 0 {
		return errors.New("图片路径非http/https")
	}
	err := util.Retry(func() error {
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		req.Header.Set("Connection", "keep-alive")

		res, err := h.client.Get(url)
		if err != nil {
			return err
		}
		defer func() {
			err := res.Body.Close()
			if err != nil {
				logger.LOG_WARN("下载图片,关闭res失败：url - "+url, err)
			}
		}()
		bytes, err := ioutil.ReadAll(res.Body)
		if err != nil {
			return err
		}
		if len(bytes) > 0 {
			image.Data = base64.Encode(bytes)
			image.StoragePath = ""
		}
		return nil
	}, 3, 100*time.Millisecond)

	return err
}

func (h *ImageDownloader) InitImageUrlConvert() {
	ruleStr := strings.TrimSpace(h.convertUrlRules)
	ruleStrs := strings.Split(ruleStr, "|")
	rules := make([][]string, 0, len(ruleStrs))
	for _, str := range ruleStrs {
		str = strings.TrimSpace(str)
		if str == "" {
			continue
		}
		rule := strings.Split(str, ",")
		rules = append(rules, rule)
	}
	if len(rules) == 0 {
		h.imageUrlCovert = nil
		return
	}
	h.imageUrlCovert = func(url string) string {
		for _, rule := range rules {
			switch len(rule) {
			case 2:
				url = strings.Replace(url, rule[0], rule[1], 1)
			default:
			}
		}
		return url
	}
}

func (h *ImageDownloader) convertUrl(url string) string {
	if url == "" {
		return url
	}
	if h.imageUrlCovert == nil {
		return url
	}
	logger.LOG_DEBUG(fmt.Sprintf("转换前的url：%s，url.length：%d", url, len(url)))
	url = h.imageUrlCovert(url)
	logger.LOG_DEBUG(fmt.Sprintf("转换后的url：%s，url.length：%d", url, len(url)))
	return url
}

func (h *ImageDownloader) Close() error {
	if h.executor != nil {
		h.executor.Close()
	}
	return nil
}
