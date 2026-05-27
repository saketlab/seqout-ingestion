package config

import (
	"flag"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/saketlab/seqout-ingestion/internal/utils"
)

var validStageOptions = []string{"ingestion", "all"}
var validDatasourceOptions = []string{"geo", "sra", "ena", "ae", "all"}
var validStageOptionsStr = strings.Join(validStageOptions, ",")
var validDatasourceOptionsStr = strings.Join(validDatasourceOptions, ",")

type Config struct {
	Stage                   string
	Datasource              string
	OutputDir               string
	startDateRaw            string
	endDateRaw              string
	StartDate               time.Time
	EndDate                 time.Time
	SRAAccessionsTabFileTTL time.Duration
	NumWorkers              int
}

func (c *Config) validate() error {
	stage := strings.ToLower(c.Stage)
	datasource := strings.ToLower(c.Datasource)

	if !slices.Contains(validStageOptions, stage) {
		return fmt.Errorf("invalid stage. valid options: %s", validStageOptionsStr)
	}

	if c.OutputDir == "" {
		return fmt.Errorf("missing output dir flag")
	}

	if c.Stage == "ingestion" {
		if c.startDateRaw == "" {
			c.StartDate = time.Now()
		} else {
			t, err := utils.FromDateStrToTime(c.startDateRaw)
			if err != nil {
				return fmt.Errorf("failed to parse start date flag: %w", err)
			}

			c.StartDate = t
		}

		if c.endDateRaw == "" {
			c.EndDate = time.Now()
		} else {
			t, err := utils.FromDateStrToTime(c.endDateRaw)
			if err != nil {
				return fmt.Errorf("failed to parse end date flag: %w", err)
			}

			c.EndDate = t
		}

		if c.StartDate.After(c.EndDate) {
			return fmt.Errorf("start date must be before end date")
		}

		if !slices.Contains(validDatasourceOptions, datasource) {
			return fmt.Errorf("invalid data source for ingestion stage. valid options: %s", validDatasourceOptionsStr)
		}
	}

	return nil
}

func ParseConfig() (Config, error) {
	var cfg Config

	flag.StringVar(&cfg.Stage, "stage", "all", fmt.Sprintf("stage of the pipeline to run (options: %s)", strings.Join(validStageOptions, ",")))
	flag.StringVar(&cfg.Datasource, "datasource", "all", fmt.Sprintf("name of the data source from where to pull data (options: %s)", strings.Join(validDatasourceOptions, ",")))
	flag.StringVar(&cfg.OutputDir, "output-dir", "/NAS/pysraweb", "directory where all the dump files would be stored")
	flag.StringVar(&cfg.startDateRaw, "start-date", "", "start date of dump range to ingest. format: YY-MM-DD")
	flag.StringVar(&cfg.endDateRaw, "end-date", "", "end date of dump range to ingest. format: YY-MM-DD")
	flag.DurationVar(&cfg.SRAAccessionsTabFileTTL, "sra-tab-ttl", 2*24*time.Hour, "ttl for `SRA_Accessions.tab` file")
	flag.IntVar(&cfg.NumWorkers, "num-workers", runtime.NumCPU()-2, "number of workers which would be used for concurrent stuff")
	flag.Parse()

	return cfg, cfg.validate()
}
