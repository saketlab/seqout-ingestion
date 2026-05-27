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
	chunks := make([]FileChunk, numWorkers)

	var g errgroup.Group
	bar := progressbar.DefaultBytes(fileSize, fmt.Sprintf("downloading %s...", url))

	for i := range numWorkers {
		g.Go(func() error {
			start := int64(i) * chunkSize
			end := start + chunkSize - 1
			if i == numWorkers-1 {
				end = fileSize - 1
			}

			data, err := downloadChunk(url, start, end, bar)
			if err != nil {
				return err
			}

			chunks[i] = FileChunk{
				Index: i,
				Data:  data,
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("failed to download file in chunks: %w", err)
	}

	out, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("failed to create dest path: %w", err)
	}
	defer out.Close()

	for i, c := range chunks {
		if _, err := out.Write(c.Data); err != nil {
			return fmt.Errorf("failed to write data into dest path for chunk #%d: %w", i+1, err)
		}
	}

	return nil
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

func downloadChunk(url string, start, end int64, bar *progressbar.ProgressBar) ([]byte, error) {
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send req: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent {
		return nil, fmt.Errorf("unexpected status: %s (%d)", resp.Status, resp.StatusCode)
	}

	reader := io.TeeReader(resp.Body, bar)
	return io.ReadAll(reader)
}
