CREATE INDEX IF NOT EXISTS idx_dl_study ON run_download_links(study_accession);
CREATE INDEX IF NOT EXISTS idx_dl_fastq_ftp ON run_download_links(fastq_ftp);
CREATE INDEX IF NOT EXISTS idx_dl_ncbi_sra_url ON run_download_links(ncbi_sra_url);
CREATE INDEX IF NOT EXISTS idx_dl_ena_ncbi ON run_download_links(fastq_ftp, ncbi_sra_url);
