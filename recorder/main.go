package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/grafov/m3u8"
)

type Segment struct {
	URL string
	Seq uint64
}

type Downloaded struct {
	Seq  uint64
	Data []byte
}

func main() {
	// ログの出力先を標準出力に設定
	log.SetOutput(os.Stdout)
	log.Println("Starting go-stream-capturer...")

	m3uURL := flag.String("url", "", "m3u8 url")
	minutes := flag.Int("minutes", 0, "recording minutes")
	split := flag.Int("split", 600, "split seconds")
	outDir := flag.String("out", "out", "output prefix")
	tag := flag.String("tag", "", "github release tag for auto upload")
	startStr := flag.String("start", "", "start time (YYYY-MM-DD HH:MM in JST)")
	flag.Parse()

	if *m3uURL == "" || *minutes == 0 {
		log.Fatal("Error: url and minutes are required")
	}

	log.Printf("Target URL: %s\n", *m3uURL)
	log.Printf("Recording Duration: %d minutes\n", *minutes)
	log.Printf("File Split Interval: %d seconds\n", *split)
	log.Printf("Release Tag: %s\n", *tag)
	if *startStr != "" {
		log.Printf("Scheduled Start Time (JST): %s\n", *startStr)
	}

	// === 開始時間までの待機処理（1ミリ秒の精度で0秒に合わせる） ===
	if *startStr != "" {
		loc := time.FixedZone("JST", 9*60*60)
		startTime, err := time.ParseInLocation("2006-01-02 15:04", *startStr, loc)
		if err != nil {
			log.Printf("Warning: failed to parse start time (%s): %v\n", *startStr, err)
		} else {
			now := time.Now().In(loc)
			if now.Before(startTime) {
				waitDur := startTime.Sub(now)
				log.Printf("Waiting %v until recording start time (precisely at 00 seconds)...\n", waitDur)
				time.Sleep(waitDur)
				log.Println("Start time reached (00 seconds). Initiating recording immediately!")
			} else {
				log.Println("Start time has already passed. Initiating recording immediately.")
			}
		}
	}

	// 録画時間（minutes）のタイムアウト設定
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*minutes)*time.Minute)
	defer cancel()

	resolvedURL, err := resolveMaster(*m3uURL)
	if err != nil {
		log.Fatalf("Failed to resolve master playlist: %v", err)
	}
	log.Printf("Resolved Media Playlist URL: %s\n", resolvedURL)

	taskChan := make(chan *Segment, 100)
	resultChan := make(chan *Downloaded, 100)
	firstSeqChan := make(chan uint64, 1)

	var wg sync.WaitGroup

	// ループA: プレイリスト監視 (待機なし・即時実行)
	wg.Add(1)
	go monitorPlaylist(ctx, &wg, resolvedURL, taskChan, firstSeqChan)

	// ループB: ダウンローダー6並列
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go downloader(ctx, &wg, i+1, taskChan, resultChan)
	}

	// 書き込み・順序管理・ファイルローテーション・逐次アップロード
	wg.Add(1)
	go writer(ctx, &wg, resultChan, firstSeqChan, *outDir, *split, *tag)

	wg.Wait()
	log.Println("Recording successfully finished and all files are processed.")
}

func resolveMaster(playlistURL string) (string, error) {
	client := &http.Client{Timeout: 15 * time.Second}
	req, err := http.NewRequest("GET", playlistURL, nil)
	if err != nil {
		return "", err
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	p, listType, err := m3u8.DecodeFrom(resp.Body, true)
	if err != nil {
		return "", err
	}

	if listType == m3u8.MASTER {
		master := p.(*m3u8.MasterPlaylist)
		var maxBW uint32
		var bestURI string
		for _, v := range master.Variants {
			if v.VariantParams.Bandwidth > maxBW {
				maxBW = v.VariantParams.Bandwidth
				bestURI = v.URI
			}
		}
		if bestURI == "" {
			return playlistURL, nil
		}
		base, _ := url.Parse(playlistURL)
		u, _ := url.Parse(bestURI)
		return base.ResolveReference(u).String(), nil
	}
	return playlistURL, nil
}

func monitorPlaylist(ctx context.Context, wg *sync.WaitGroup, playlistURL string, taskChan chan<- *Segment, firstSeqChan chan<- uint64) {
	defer wg.Done()
	client := &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{
			DialContext: (&net.Dialer{Timeout: 10 * time.Second}).DialContext,
		},
	}

	seen := make(map[uint64]bool)
	var mu sync.Mutex
	first := true

	// プレイリストを取得してセグメントを抽出するインナー関数（0秒での即時実行用）
	fetchAndProcess := func() {
		req, err := http.NewRequestWithContext(ctx, "GET", playlistURL, nil)
		if err != nil {
			log.Printf("[Monitor] Request creation failed: %v\n", err)
			return
		}
		resp, err := client.Do(req)
		if err != nil {
			log.Printf("[Monitor] Playlist fetch failed: %v\n", err)
			return
		}

		p, listType, err := m3u8.DecodeFrom(resp.Body, true)
		resp.Body.Close()
		if err != nil || listType != m3u8.MEDIA {
			return
		}

		media := p.(*m3u8.MediaPlaylist)
		baseURL, _ := url.Parse(playlistURL)

		mu.Lock()
		var minSeq uint64 = math.MaxUint64
		var found bool

		for _, seg := range media.Segments {
			if seg == nil {
				continue
			}
			seq := media.SeqNo + seg.SeqId
			if !seen[seq] {
				seen[seq] = true
				u, _ := url.Parse(seg.URI)
				segURL := baseURL.ResolveReference(u).String()
				
				log.Printf("[Monitor] Found new segment -> Seq: %d\n", seq)
				taskChan <- &Segment{URL: segURL, Seq: seq}

				if first {
					if seq < minSeq {
						minSeq = seq
						found = true
					}
				}
			}
		}
		if first && found {
			log.Printf("[Monitor] First segment sequence detected: %d\n", minSeq)
			firstSeqChan <- minSeq
			first = false
		}
		mu.Unlock()
	}

	// 1回目のセグメント取得を開始時刻ちょうど（0秒）に即座に実行
	fetchAndProcess()

	// 2秒おきの監視サイクルを設定
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("[Monitor] Stop monitoring playlist.")
			return
		case <-ticker.C:
			fetchAndProcess()
		}
	}
}

func downloader(ctx context.Context, wg *sync.WaitGroup, workerID int, taskChan <-chan *Segment, resultChan chan<- *Downloaded) {
	defer wg.Done()
	client := &http.Client{Timeout: 45 * time.Second}

	for {
		select {
		case <-ctx.Done():
			log.Printf("[Downloader-%d] Stop working.\n", workerID)
			return
		case task, ok := <-taskChan:
			if !ok {
				return
			}
			data := downloadWithRetry(ctx, client, task.URL)
			if data != nil {
				log.Printf("[Downloader-%d] Success Seq: %d (Size: %d bytes)\n", workerID, task.Seq, len(data))
			} else {
				log.Printf("[Downloader-%d] Failed Seq: %d (Skipped)\n", workerID, task.Seq)
			}
			
			select {
			case <-ctx.Done():
				return
			case resultChan <- &Downloaded{Seq: task.Seq, Data: data}:
			}
		}
	}
}

func downloadWithRetry(ctx context.Context, client *http.Client, targetURL string) []byte {
	retryCount := 0
	notFoundCount := 0

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		req, err := http.NewRequestWithContext(ctx, "GET", targetURL, nil)
		if err == nil {
			resp, err := client.Do(req)
			if err == nil {
				if resp.StatusCode == 200 {
					data, err := io.ReadAll(resp.Body)
					resp.Body.Close()
					if err == nil {
						return data
					}
				} else if resp.StatusCode == 404 || resp.StatusCode == 410 {
					resp.Body.Close()
					notFoundCount++
					if notFoundCount >= 3 {
						return nil
					}
				} else {
					resp.Body.Close()
				}
			} else {
				log.Printf("[Retry] Network error on download: %v\n", err)
			}
		}

		retryCount++
		delay := float64(2) * math.Pow(2, float64(retryCount-1))
		if delay > 30 {
			delay = 30
		}
		jitter := rand.Float64() * 3
		sleepDur := time.Duration((delay + jitter) * float64(time.Second))

		log.Printf("[Retry] Waiting %v to retry downloading...\n", sleepDur)

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(sleepDur):
		}
	}
}

func writer(ctx context.Context, wg *sync.WaitGroup, resultChan <-chan *Downloaded, firstSeqChan <-chan uint64, outPrefix string, splitSec int, tag string) {
	defer wg.Done()
	buffer := make(map[uint64][]byte)
	var nextSeq uint64
	firstSeqSet := false

	var currentFile *os.File
	var currentFileIdx int
	var fileOpenTime time.Time
	var currentFilename string

	var uploadWg sync.WaitGroup

	closeAndUpload := func(f *os.File, fname string) {
		f.Close()
		log.Printf("[Writer] Closed file: %s\n", fname)
		if tag != "" {
			uploadWg.Add(1)
			go func(name string) {
				defer uploadWg.Done()
				log.Printf("[Uploader] Start uploading %s to release %s\n", name, tag)
				
				repo := os.Getenv("GH_REPO")
				if repo == "" {
					repo = os.Getenv("GITHUB_REPOSITORY")
				}

				for retry := 0; retry < 3; retry++ {
					args := []string{"release", "upload", tag, name, "--clobber"}
					if repo != "" {
						args = append(args, "-R", repo)
					}
					
					cmd := exec.Command("gh", args...)
					cmd.Stdout = os.Stdout
					cmd.Stderr = os.Stderr
					err := cmd.Run()
					if err == nil {
						log.Printf("[Uploader] Successfully uploaded %s\n", name)
						os.Remove(name) // アップロード成功後にローカルファイルを削除
						return
					}
					log.Printf("[Uploader] Failed to upload %s (attempt %d/3): %v\n", name, retry+1, err)
					time.Sleep(5 * time.Second)
				}
			}(fname)
		}
	}

	closeCurrentFile := func() {
		if currentFile != nil {
			closeAndUpload(currentFile, currentFilename)
			currentFile = nil
		}
	}

	defer func() {
		closeCurrentFile()
		uploadWg.Wait()
		log.Println("[Writer] All uploads finished.")
	}()

	checkRotation := func() error {
		if currentFile == nil || time.Since(fileOpenTime).Seconds() >= float64(splitSec) {
			closeCurrentFile()
			currentFilename = fmt.Sprintf("%s-%03d.ts", outPrefix, currentFileIdx)
			f, err := os.Create(currentFilename)
			if err != nil {
				log.Printf("[Writer] Failed to create file %s: %v\n", currentFilename, err)
				return err
			}
			currentFile = f
			currentFileIdx++
			fileOpenTime = time.Now()
			log.Printf("[Writer] Created new file: %s\n", currentFilename)
		}
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return
		case seq := <-firstSeqChan:
			nextSeq = seq
			firstSeqSet = true
			for {
				if data, ok := buffer[nextSeq]; ok {
					if len(data) > 0 {
						if err := checkRotation(); err == nil {
							currentFile.Write(data)
						}
					}
					delete(buffer, nextSeq)
					nextSeq++
				} else {
					break
				}
			}
		case res := <-resultChan:
			data := res.Data
			if data == nil {
				data = []byte{} // スキップされた場合の空データ
			}
			if firstSeqSet {
				if res.Seq < nextSeq {
					continue
				}
				buffer[res.Seq] = data
				for {
					if d, ok := buffer[nextSeq]; ok {
						if len(d) > 0 {
							if err := checkRotation(); err == nil {
								currentFile.Write(d)
							}
						}
						delete(buffer, nextSeq)
						nextSeq++
					} else {
						break
					}
				}
			} else {
				buffer[res.Seq] = data
			}
		}
	}
}
