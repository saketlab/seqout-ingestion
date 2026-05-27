package ingestion

import (
	"database/sql"
	_ "embed"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/saketlab/seqout-ingestion/internal/helpers"
	"github.com/saketlab/seqout-ingestion/internal/utils"
	"golang.org/x/net/html"
)

//go:embed sql/sra_tables.sql
var createSraTablesQuery string

const SRA_REPORTS_BASE_URL = "https://ftp.ncbi.nlm.nih.gov/sra/reports/Metadata"
const SRA_FILE_DUMP_PREFIX = "NCBI_SRA_Metadata_Full_"
const SRA_METADATA_FOLDER = "sra_metadata"
const SRA_ACCESSIONS_TAB_FILE_NAME = "SRA_Accessions.tab"

func ingestSra(cfg IngestionConfig) error {
	dateStr := utils.FromTimeToDateStr(time.Now())
	outputDir := filepath.Join(cfg.OutputDir, "sra_metadata")
	sqliteFilePath := filepath.Join(outputDir, fmt.Sprintf("sra_metadata_%s.db", dateStr))

	if err := os.MkdirAll(outputDir, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create all parent dirs: %w", err)
	}

	db, err := sql.Open("sqlite3", sqliteFilePath)
	if err != nil {
		return fmt.Errorf("failed to open sqlite db: %w", err)
	}
	defer db.Close()

	if _, err := db.Exec(createSraTablesQuery); err != nil {
		return fmt.Errorf("failed to create tables: %w", err)
	}

	_, err = extractSraFileDumpsInRange(cfg.StartDate, cfg.EndDate)
	if err != nil {
		return fmt.Errorf("failed to extract file dumps in specified range: %w", err)
	}

	if err := downloadAccessionsTabFile(outputDir, cfg.SRAAccessionsTabFileTTL, cfg.NumWorkers); err != nil {
		return fmt.Errorf("failed to download sra accessions tab file: %w", err)
	}

	return nil
}

func extractSraFileDumpsInRange(startDate, endDate time.Time) ([]string, error) {
	var files []string

	resp, err := http.Get(SRA_REPORTS_BASE_URL)
	if err != nil {
		return files, fmt.Errorf("failed to sra reports page: %w", err)
	}
	defer resp.Body.Close()

	doc, err := html.Parse(resp.Body)
	if err != nil {
		return files, fmt.Errorf("failed to parse sra reports html resp: %w", err)
	}

	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "a" {
			for _, attr := range n.Attr {
				if attr.Key != "href" {
					continue
				}

				href := attr.Val
				if !strings.HasPrefix(href, SRA_FILE_DUMP_PREFIX) {
					continue
				}

				_, dateStrWithExt, found := strings.Cut(href, SRA_FILE_DUMP_PREFIX)
				if !found {
					continue
				}

				dateStr, _, found := strings.Cut(dateStrWithExt, ".tar.gz")
				if !found {
					continue
				}

				date, err := time.Parse("20060102", dateStr) // YYYYMMDD
				if err != nil {
					continue
				}

				if !date.Before(startDate) && !date.After(endDate) {
					files = append(files, fmt.Sprintf("%s/%s", SRA_REPORTS_BASE_URL, href))
				}
			}
		}

		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}

	walk(doc)
	return files, nil
}

func downloadAccessionsTabFile(outputDir string, ttl time.Duration, numWorkers int) error {
	accessionsTabFilePath := filepath.Join(outputDir, SRA_ACCESSIONS_TAB_FILE_NAME)
	info, err := os.Stat(accessionsTabFilePath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to fetch file info for sra accessions tab file: %w", err)
	}

	if info != nil && time.Since(info.ModTime()) > ttl {
		log.Println("skipping download for sra accessions tab file as it is within ttl range")
		return nil
	}

	if err := helpers.BetterDownloader(fmt.Sprintf("%s/%s", SRA_REPORTS_BASE_URL, SRA_ACCESSIONS_TAB_FILE_NAME), accessionsTabFilePath, numWorkers); err != nil {
		return err
	}

	return nil
}
