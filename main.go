package main

import (
	"log"

	"github.com/saketlab/seqout-ingestion/internal/config"
	"github.com/saketlab/seqout-ingestion/internal/stages/ingestion"
)

func main() {
	cfg, err := config.ParseConfig()
	if err != nil {
		log.Fatalf("failed to parse config: %s", err)
	}

	if cfg.Stage == "ingestion" {
		if err := ingestion.RunIngestion(ingestion.IngestionConfig{
			Datasource:              cfg.Datasource,
			OutputDir:               cfg.OutputDir,
			StartDate:               cfg.StartDate,
			EndDate:                 cfg.EndDate,
			SRAAccessionsTabFileTTL: cfg.SRAAccessionsTabFileTTL,
			NumWorkers:              cfg.NumWorkers,
		}); err != nil {
			log.Fatalf("failed to run ingestion pipeline: %s", err)
		}
	}
}
