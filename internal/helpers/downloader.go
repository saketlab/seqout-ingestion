package helpers

import (
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/schollz/progressbar/v3"
	"golang.org/x/sync/errgroup"
)

type FileChunk struct {
	Index int
	Data  []byte
}

func BetterDownloader(url string, destPath string, numWorkers int) error {
	resp, err := http.Head(url)
	if err != nil {
		return fmt.Errorf("head request failed: %w", err)
	}
	defer resp.Body.Close()

	fileSize := resp.ContentLength
	acceptsRange := resp.Header.Get("Accept-Ranges") == "bytes"

	if fileSize <= 0 || !acceptsRange {
		return SimpleFileDownload(url, destPath) // fallback
	}

	chunkSize := fileSize / int64(numWorkers)

	out, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("failed to create dest path: %w", err)
	}
	defer out.Close()

	if err := out.Truncate(fileSize); err != nil {
		return fmt.Errorf("failed to pre-allocate file: %w", err)
	}

	var g errgroup.Group
	bar := progressbar.DefaultBytes(fileSize, fmt.Sprintf("downloading %s...", url))

	for i := range numWorkers {
		g.Go(func() error {
			start := int64(i) * chunkSize
			end := start + chunkSize - 1
			if i == numWorkers-1 {
				end = fileSize - 1
			}

			return downloadChunk(url, start, end, out, bar)
		})
	}

	return g.Wait()
}

func SimpleFileDownload(url string, destPath string) error {
	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("failed to send req: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status: %s (%d)", resp.Status, resp.StatusCode)
	}

	out, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("failed to create dest path: %w", err)
	}
	defer out.Close()

	bar := progressbar.DefaultBytes(resp.ContentLength, fmt.Sprintf("downloading %s...", url))

	_, err = io.Copy(io.MultiWriter(out, bar), resp.Body)
	return err
}

func downloadChunk(url string, start, end int64, out *os.File, bar *progressbar.ProgressBar) error {
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send req: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("unexpected status: %s (%d)", resp.Status, resp.StatusCode)
	}

	writer := io.NewOffsetWriter(out, start)
	_, err = io.Copy(io.MultiWriter(writer, bar), resp.Body)
	return err
}
