package ingestion

import (
	"fmt"
	"time"
)

type IngestionConfig struct {
	Datasource              string
	OutputDir               string
	StartDate               time.Time
	EndDate                 time.Time
	SRAAccessionsTabFileTTL time.Duration
	NumWorkers              int
}

func RunIngestion(cfg IngestionConfig) error {
	switch cfg.Datasource {
	case "sra":
		return ingestSra(cfg)
	default:
		return fmt.Errorf("unsupported datasource for ingestion pipeline: %s", cfg.Datasource)
	}
}
