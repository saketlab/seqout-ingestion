--
-- PostgreSQL database dump
--

\restrict W8Zzfk6Wj3Vv16DZOge0MC1iIVKAS43MPugSIJUzZs9cTL2d8bTAMJNwM0f3IyB

-- Dumped from database version 16.14 (Ubuntu 16.14-0ubuntu0.24.04.1)
-- Dumped by pg_dump version 16.14 (Ubuntu 16.14-0ubuntu0.24.04.1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: pg_trgm; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;


--
-- Name: EXTENSION pg_trgm; Type: COMMENT; Schema: -; Owner:
--

COMMENT ON EXTENSION pg_trgm IS 'text similarity measurement and index searching based on trigrams';


--
-- Name: vector; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public;


--
-- Name: EXTENSION vector; Type: COMMENT; Schema: -; Owner:
--

COMMENT ON EXTENSION vector IS 'vector data type and ivfflat and hnsw access methods';


--
-- Name: vectorscale; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS vectorscale WITH SCHEMA public;


--
-- Name: EXTENSION vectorscale; Type: COMMENT; Schema: -; Owner:
--

COMMENT ON EXTENSION vectorscale IS 'diskann access method for vector search';


--
-- Name: geo_series_tsv_update(); Type: FUNCTION; Schema: public; Owner: sramap
--

CREATE FUNCTION public.geo_series_tsv_update() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    NEW.tsv :=
          setweight(to_tsvector('english', coalesce(NEW.title,'')), 'A')
      ||  setweight(to_tsvector('english', coalesce(NEW.summary,'')), 'B')
      ||  setweight(to_tsvector('english', coalesce(NEW.overall_design,'')), 'C')

      ||  setweight(
              to_tsvector('english',
                  coalesce(array_to_string(NEW.contributors,' '),'')
              ), 'D')

      ||  setweight(
              to_tsvector('english',
                  coalesce(array_to_string(NEW.departments,' '),'')
              ), 'D')

      ||  setweight(
              to_tsvector('english',
                  coalesce(array_to_string(NEW.organizations,' '),'')
              ), 'D')

      ||  setweight(
              to_tsvector('english',
                  coalesce(array_to_string(NEW.countries,' '),'')
              ), 'D');

    RETURN NEW;
END
$$;


ALTER FUNCTION public.geo_series_tsv_update() OWNER TO sramap;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: arrayexpress_experiments; Type: TABLE; Schema: public; Owner: sramap
--

CREATE TABLE public.arrayexpress_experiments (
    accession text NOT NULL,
    title text,
    organism text,
    description text,
    study_type text,
    release_date date,
    submission_date date,
    experimental_factors text[],
    experimental_factor_types text[],
    contact_name text,
    contact_email text,
    contact_affiliation text,
    pubmed_id text,
    pubmed_id_pysradb text,
    pubmed_last_checked date,
    protocol_names text[],
    protocol_types text[],
    attributes_json jsonb,
    organisms text[],
    publication_title text,
    publication_authors text,
    publication_doi text,
    external_links jsonb,
    processed_data_files jsonb,
    experimental_factors_detail jsonb,
    authors text[],
    organizations jsonb,
    protocols_description text,
    assay_count integer,
    technology text,
    assay_by_molecule text,
    minseqe_score jsonb,
    organizations_text text,
    alias text,
    authors_text text,
    tsv tsvector GENERATED ALWAYS AS ((((setweight(to_tsvector('english'::regconfig, COALESCE(title, ''::text)), 'A'::"char") || setweight(to_tsvector('english'::regconfig, COALESCE(description, ''::text)), 'B'::"char")) || setweight(to_tsvector('english'::regconfig, COALESCE(organizations_text, ''::text)), 'C'::"char")) || setweight(to_tsvector('english'::regconfig, COALESCE(authors_text, ''::text)), 'D'::"char"))) STORED,
    countries text[],
    coords_3d double precision[],
    neighbors jsonb[],
    coords_2d double precision[],
    clusters jsonb
);


ALTER TABLE public.arrayexpress_experiments OWNER TO sramap;

--
-- Name: arrayexpress_geocode; Type: TABLE; Schema: public; Owner: sramap
--

CREATE TABLE public.arrayexpress_geocode (
    organization text,
    address_line text,
    address_city text,
    address_state text,
    address_zipcode text,
    address_country text,
    n_experiments integer,
    address_key text NOT NULL,
    geo_query text,
    geo_formatted_address text,
    geo_place_name text,
    geo_longitude double precision,
    geo_latitude double precision,
    geo_city text,
    geo_district text,
    geo_subdistrict text,
    geo_neighbourhood text,
    geo_state text,
    geo_postcode text,
    geo_country text,
    geo_source text,
    geo_country_mismatch boolean,
    geo_error text,
    search_vector tsvector GENERATED ALWAYS AS (to_tsvector('english'::regconfig, ((((COALESCE(organization, ''::text) || ' '::text) || COALESCE(geo_place_name, ''::text)) || ' '::text) || COALESCE(geo_formatted_address, ''::text)))) STORED
);


ALTER TABLE public.arrayexpress_geocode OWNER TO sramap;

--
-- Name: arrayexpress_samples; Type: TABLE; Schema: public; Owner: sramap
--

CREATE TABLE public.arrayexpress_samples (
    id integer NOT NULL,
    experiment_accession text NOT NULL,
    source_name text,
    organism text,
    organism_part text,
    cell_type text,
    genotype text,
    sample_source_name text,
    sample_title text,
    sample_description text,
    library_strategy text,
    library_source text,
    library_selection text,
    ena_experiment text,
    ena_run text,
    attributes_json jsonb
);


ALTER TABLE public.arrayexpress_samples OWNER TO sramap;

--
-- Name: arrayexpress_samples_id_seq; Type: SEQUENCE; Schema: public; Owner: sramap
--

CREATE SEQUENCE public.arrayexpress_samples_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.arrayexpress_samples_id_seq OWNER TO sramap;

--
-- Name: arrayexpress_samples_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: sramap
--

ALTER SEQUENCE public.arrayexpress_samples_id_seq OWNED BY public.arrayexpress_samples.id;


--
-- Name: cellosaurus_samples_v3; Type: TABLE; Schema: public; Owner: sramap
--

CREATE TABLE public.cellosaurus_samples_v3 (
    study_accession text NOT NULL,
    sample text NOT NULL,
    age text,
    sex text,
    ethnicity text,
    phenotype text,
    cell_type text,
    tissue text,
    strain text,
    disease text,
    assay text,
    assay_category text,
    cell_line text,
    treatment text,
    development_stage text,
    sample_type text,
    genetic_modification text,
    organism text,
    taxid text,
    tissue_primary_site text,
    tissue_site_type text
);


ALTER TABLE public.cellosaurus_samples_v3 OWNER TO sramap;

--
-- Name: cluster_labels; Type: TABLE; Schema: public; Owner: sramap
--

CREATE TABLE public.cluster_labels (
    level text NOT NULL,
    cluster_id integer NOT NULL,
    label text,
    size integer,
    centroid_x double precision,
    centroid_y double precision
);


ALTER TABLE public.cluster_labels OWNER TO sramap;

--
-- Name: clusters; Type: TABLE; Schema: public; Owner: sramap
--

CREATE TABLE public.clusters (
    title text,
    centroid real[],
    num_points integer,
    CONSTRAINT centroid_2d_check CHECK (((centroid IS NULL) OR (array_length(centroid, 1) = 2)))
);


ALTER TABLE public.clusters OWNER TO sramap;

--
-- Name: common_names; Type: TABLE; Schema: public; Owner: sramap
--

CREATE TABLE public.common_names (
    tax_id integer,
    scientific_name text,
    common_name text
);


ALTER TABLE public.common_names OWNER TO sramap;

--
-- Name: cross_ref_ae; Type: TABLE; Schema: public; Owner: sramap
--

CREATE TABLE public.cross_ref_ae (
    ae_accession text NOT NULL,
    target_accession text NOT NULL,
    link_type text NOT NULL
);


ALTER TABLE public.cross_ref_ae OWNER TO sramap;

--
-- Name: cross_ref_ena; Type: TABLE; Schema: public; Owner: sramap
--

CREATE TABLE public.cross_ref_ena (
    ena_accession text NOT NULL,
    target_accession text NOT NULL,
    link_type text NOT NULL
);


ALTER TABLE public.cross_ref_ena OWNER TO sramap;

--
-- Name: cross_ref_geo; Type: TABLE; Schema: public; Owner: sramap
--

CREATE TABLE public.cross_ref_geo (
    geo_accession text NOT NULL,
    target_accession text NOT NULL,
    link_type text NOT NULL
);


ALTER TABLE public.cross_ref_geo OWNER TO sramap;

--
-- Name: cross_ref_pubmed; Type: TABLE; Schema: public; Owner: sramap
--

CREATE TABLE public.cross_ref_pubmed (
    accession text NOT NULL,
    pubmed_id text NOT NULL,
    source text NOT NULL
);


ALTER TABLE public.cross_ref_pubmed OWNER TO sramap;

--
-- Name: ena_experiments; Type: TABLE; Schema: public; Owner: sramap
--

CREATE TABLE public.ena_experiments (
    run_accession text NOT NULL,
    experiment_accession text NOT NULL,
    sample_accession text,
    study_accession text NOT NULL,
    experiment_title text,
    library_strategy text,
    library_source text,
    library_selection text,
    library_layout text,
    library_name text,
    instrument_platform text,
    instrument_model text,
    scientific_name text,
    tax_id integer,
    center_name text,
    first_public date,
    last_updated date,
    sample_title text,
    sample_alias text,
    sample_description text,
    country text,
    host text,
    tissue_type text,
    cell_type text,
    disease text,
    dev_stage text,
    base_count bigint,
    read_count bigint,
    run_alias text
);


ALTER TABLE public.ena_experiments OWNER TO sramap;

--
-- Name: ena_geocode; Type: TABLE; Schema: public; Owner: sramap
--

CREATE TABLE public.ena_geocode (
    center_name text NOT NULL,
    formatted_address text,
    longitude double precision,
    latitude double precision,
    city text,
    district text,
    subdistrict text,
    neighbourhood text,
    state text,
    postcode text,
    country text,
    source text,
    error text,
    n_experiments integer,
    n_studies integer
);


ALTER TABLE public.ena_geocode OWNER TO sramap;

--
-- Name: ena_samples; Type: TABLE; Schema: public; Owner: sramap
--

CREATE TABLE public.ena_samples (
    sample_accession text NOT NULL,
    secondary_sample_accession text,
    sample_alias text,
    sample_title text,
    sample_description text,
    tax_id integer,
    scientific_name text,
    study_accession text NOT NULL,
    center_name text,
    broker_name text,
    first_public date,
    last_updated date,
    submission_accession text,
    submission_tool text,
    status text,
    project_name text,
    description text,
    checklist text,
    ncbi_reporting_standard text,
    keywords text,
    tag text,
    datahub text,
    strain text,
    cultivar text,
    ecotype text,
    isolate text,
    isolation_source text,
    bio_material text,
    culture_collection text,
    specimen_voucher text,
    sub_species text,
    sub_strain text,
    variety text,
    mating_type text,
    sex text,
    germline text,
    collection_date text,
    collection_date_start date,
    collection_date_end date,
    collected_by text,
    identified_by text,
    country text,
    lat text,
    lon text,
    location text,
    location_start text,
    location_end text,
    altitude text,
    depth text,
    elevation text,
    marine_region text,
    sampling_campaign text,
    sampling_platform text,
    sampling_site text,
    sample_collection text,
    sample_material text,
    sample_capture_status text,
    protocol_label text,
    environmental_sample text,
    environment_biome text,
    environment_feature text,
    environment_material text,
    broad_scale_environmental_context text,
    local_environmental_context text,
    environmental_medium text,
    investigation_type text,
    ph text,
    salinity text,
    temperature text,
    host text,
    host_body_site text,
    host_genotype text,
    host_gravidity text,
    host_growth_conditions text,
    host_phenotype text,
    host_scientific_name text,
    host_sex text,
    host_status text,
    host_tax_id text,
    submitted_host_sex text,
    cell_line text,
    cell_type text,
    dev_stage text,
    disease text,
    tissue_lib text,
    tissue_type text,
    age text,
    experimental_factor text,
    serotype text,
    serovar text,
    surveillance_target text,
    target_gene text,
    assembly_quality text,
    assembly_software text,
    binning_software text,
    completeness_score text,
    contamination_score text,
    sequencing_method text,
    tax_lineage text,
    taxonomic_classification text,
    taxonomic_identity_marker text,
    related_sample_accession text
);


ALTER TABLE public.ena_samples OWNER TO sramap;

--
-- Name: ena_studies; Type: TABLE; Schema: public; Owner: sramap
--

CREATE TABLE public.ena_studies (
    study_accession text NOT NULL,
    secondary_study_accession text,
    secondary_study_alias text,
    secondary_study_center_name text,
    study_alias text,
    study_title text,
    study_description text,
    study_name text,
    center_name text,
    broker_name text,
    project_name text,
    tax_id integer,
    tax_lineage text,
    tax_division text,
    scientific_name text,
    strain text,
    breed text,
    cultivar text,
    isolate text,
    first_public date,
    last_updated date,
    geo_accession text,
    keywords text,
    tag text,
    submission_tool text,
    parent_study_accession text,
    description text,
    status text,
    pubmed_id_pysradb text,
    has_sra_match boolean DEFAULT false
);


ALTER TABLE public.ena_studies OWNER TO sramap;

--
-- Name: ena_studies_search; Type: TABLE; Schema: public; Owner: sramap
--

CREATE TABLE public.ena_studies_search (
    accession text,
    study_title text,
    study_description text,
    study_name text,
    center_name text,
    project_name text,
    last_updated date,
    first_public date,
    tsv tsvector GENERATED ALWAYS AS (((((setweight(to_tsvector('english'::regconfig, COALESCE(study_title, ''::text)), 'A'::"char") || setweight(to_tsvector('english'::regconfig, COALESCE(study_description, ''::text)), 'B'::"char")) || setweight(to_tsvector('english'::regconfig, COALESCE(study_name, ''::text)), 'C'::"char")) || setweight(to_tsvector('english'::regconfig, COALESCE(project_name, ''::text)), 'C'::"char")) || setweight(to_tsvector('english'::regconfig, COALESCE(center_name, ''::text)), 'D'::"char"))) STORED
);


ALTER TABLE public.ena_studies_search OWNER TO sramap;

--
-- Name: ontology_samples_v3; Type: TABLE; Schema: public; Owner: sramap
--

CREATE TABLE public.ontology_samples_v3 (
    study_accession text NOT NULL,
    sample text NOT NULL,
    age text,
    sex text,
    ethnicity text,
    phenotype text,
    cell_type text,
    tissue text,
    strain text,
    disease text,
    assay text,
    assay_category text,
    cell_line text,
    treatment text,
    development_stage text,
    sample_type text,
    genetic_modification text,
    organism text,
    taxid text,
    tissue_primary_site text,
    tissue_site_type text,
    disease_ontology_id text,
    disease_ontology_name text,
    disease_ontology_score text,
    disease_match_method text,
    disease_ancestors text,
    tissue_ontology_id text,
    tissue_ontology_name text,
    tissue_ontology_score text,
    tissue_match_method text,
    tissue_ancestors text,
    cell_type_ontology_id text,
    cell_type_ontology_name text,
    cell_type_ontology_score text,
    cell_type_match_method text,
    cell_type_ancestors text,
    assay_ontology_id text,
    assay_ontology_name text,
    assay_ontology_score text,
    assay_match_method text,
    assay_ancestors text,
    development_stage_ontology_id text,
    development_stage_ontology_name text,
    development_stage_ontology_score text,
    development_stage_match_method text,
    development_stage_ancestors text,
    cell_count integer,
    gene_count integer,
    cell_count_estimated boolean
);


ALTER TABLE public.ontology_samples_v3 OWNER TO sramap;

--
-- Name: enriched_column_top_values; Type: MATERIALIZED VIEW; Schema: public; Owner: sramap
--

CREATE MATERIALIZED VIEW public.enriched_column_top_values AS
 SELECT column_name,
    value,
    n
   FROM ( SELECT g.column_name,
            g.value,
            g.n,
            row_number() OVER (PARTITION BY g.column_name ORDER BY g.n DESC) AS rk
           FROM ( SELECT 'tissue'::text AS column_name,
                    ontology_samples_v3.tissue AS value,
                    count(*) AS n
                   FROM public.ontology_samples_v3
                  WHERE ((ontology_samples_v3.tissue IS NOT NULL) AND (ontology_samples_v3.tissue <> ''::text))
                  GROUP BY ontology_samples_v3.tissue
                UNION ALL
                 SELECT 'cell_type'::text AS column_name,
                    ontology_samples_v3.cell_type AS value,
                    count(*) AS n
                   FROM public.ontology_samples_v3
                  WHERE ((ontology_samples_v3.cell_type IS NOT NULL) AND (ontology_samples_v3.cell_type <> ''::text))
                  GROUP BY ontology_samples_v3.cell_type
                UNION ALL
                 SELECT 'disease'::text AS column_name,
                    ontology_samples_v3.disease AS value,
                    count(*) AS n
                   FROM public.ontology_samples_v3
                  WHERE ((ontology_samples_v3.disease IS NOT NULL) AND (ontology_samples_v3.disease <> ''::text))
                  GROUP BY ontology_samples_v3.disease
                UNION ALL
                 SELECT 'assay'::text AS column_name,
                    ontology_samples_v3.assay AS value,
                    count(*) AS n
                   FROM public.ontology_samples_v3
                  WHERE ((ontology_samples_v3.assay IS NOT NULL) AND (ontology_samples_v3.assay <> ''::text))
                  GROUP BY ontology_samples_v3.assay
                UNION ALL
                 SELECT 'assay_category'::text AS column_name,
                    ontology_samples_v3.assay_category AS value,
                    count(*) AS n
                   FROM public.ontology_samples_v3
                  WHERE ((ontology_samples_v3.assay_category IS NOT NULL) AND (ontology_samples_v3.assay_category <> ''::text))
                  GROUP BY ontology_samples_v3.assay_category
                UNION ALL
                 SELECT 'sex'::text AS column_name,
                    ontology_samples_v3.sex AS value,
                    count(*) AS n
                   FROM public.ontology_samples_v3
                  WHERE ((ontology_samples_v3.sex IS NOT NULL) AND (ontology_samples_v3.sex <> ''::text))
                  GROUP BY ontology_samples_v3.sex
                UNION ALL
                 SELECT 'organism'::text AS column_name,
                    ontology_samples_v3.organism AS value,
                    count(*) AS n
                   FROM public.ontology_samples_v3
                  WHERE ((ontology_samples_v3.organism IS NOT NULL) AND (ontology_samples_v3.organism <> ''::text))
                  GROUP BY ontology_samples_v3.organism
                UNION ALL
                 SELECT 'development_stage'::text AS column_name,
                    ontology_samples_v3.development_stage AS value,
                    count(*) AS n
                   FROM public.ontology_samples_v3
                  WHERE ((ontology_samples_v3.development_stage IS NOT NULL) AND (ontology_samples_v3.development_stage <> ''::text))
                  GROUP BY ontology_samples_v3.development_stage
                UNION ALL
                 SELECT 'sample_type'::text AS column_name,
                    ontology_samples_v3.sample_type AS value,
                    count(*) AS n
                   FROM public.ontology_samples_v3
                  WHERE ((ontology_samples_v3.sample_type IS NOT NULL) AND (ontology_samples_v3.sample_type <> ''::text))
                  GROUP BY ontology_samples_v3.sample_type
                UNION ALL
                 SELECT 'strain'::text AS column_name,
                    ontology_samples_v3.strain AS value,
                    count(*) AS n
                   FROM public.ontology_samples_v3
                  WHERE ((ontology_samples_v3.strain IS NOT NULL) AND (ontology_samples_v3.strain <> ''::text))
                  GROUP BY ontology_samples_v3.strain
                UNION ALL
                 SELECT 'treatment'::text AS column_name,
                    ontology_samples_v3.treatment AS value,
                    count(*) AS n
                   FROM public.ontology_samples_v3
                  WHERE ((ontology_samples_v3.treatment IS NOT NULL) AND (ontology_samples_v3.treatment <> ''::text))
                  GROUP BY ontology_samples_v3.treatment
                UNION ALL
                 SELECT 'cell_line'::text AS column_name,
                    ontology_samples_v3.cell_line AS value,
                    count(*) AS n
                   FROM public.ontology_samples_v3
                  WHERE ((ontology_samples_v3.cell_line IS NOT NULL) AND (ontology_samples_v3.cell_line <> ''::text))
                  GROUP BY ontology_samples_v3.cell_line
                UNION ALL
                 SELECT 'ethnicity'::text AS column_name,
                    ontology_samples_v3.ethnicity AS value,
                    count(*) AS n
                   FROM public.ontology_samples_v3
                  WHERE ((ontology_samples_v3.ethnicity IS NOT NULL) AND (ontology_samples_v3.ethnicity <> ''::text))
                  GROUP BY ontology_samples_v3.ethnicity
                UNION ALL
                 SELECT 'phenotype'::text AS column_name,
                    ontology_samples_v3.phenotype AS value,
                    count(*) AS n
                   FROM public.ontology_samples_v3
                  WHERE ((ontology_samples_v3.phenotype IS NOT NULL) AND (ontology_samples_v3.phenotype <> ''::text))
                  GROUP BY ontology_samples_v3.phenotype
                UNION ALL
                 SELECT 'genetic_modification'::text AS column_name,
                    ontology_samples_v3.genetic_modification AS value,
                    count(*) AS n
                   FROM public.ontology_samples_v3
                  WHERE ((ontology_samples_v3.genetic_modification IS NOT NULL) AND (ontology_samples_v3.genetic_modification <> ''::text))
                  GROUP BY ontology_samples_v3.genetic_modification
                UNION ALL
                 SELECT 'tissue_primary_site'::text AS column_name,
                    ontology_samples_v3.tissue_primary_site AS value,
                    count(*) AS n
                   FROM public.ontology_samples_v3
                  WHERE ((ontology_samples_v3.tissue_primary_site IS NOT NULL) AND (ontology_samples_v3.tissue_primary_site <> ''::text))
                  GROUP BY ontology_samples_v3.tissue_primary_site
                UNION ALL
                 SELECT 'tissue_site_type'::text AS column_name,
                    ontology_samples_v3.tissue_site_type AS value,
                    count(*) AS n
                   FROM public.ontology_samples_v3
                  WHERE ((ontology_samples_v3.tissue_site_type IS NOT NULL) AND (ontology_samples_v3.tissue_site_type <> ''::text))
                  GROUP BY ontology_samples_v3.tissue_site_type) g) ranked
  WHERE (rk <= 100)
  WITH NO DATA;


ALTER MATERIALIZED VIEW public.enriched_column_top_values OWNER TO sramap;

--
-- Name: unified_metadata; Type: TABLE; Schema: public; Owner: sramap
--

CREATE TABLE public.unified_metadata (
    canonical_accession text NOT NULL,
    source text NOT NULL,
    title text,
    description text,
    aliases jsonb,
    organism_counts jsonb,
    common_name_counts jsonb,
    dominant_scientific_name text,
    dominant_common_name text,
    library_strategy_counts jsonb,
    assay_l1_counts jsonb,
    assay_l2_counts jsonb,
    dominant_assay_l1 text,
    dominant_assay_l2 text,
    n_experiments integer,
    n_samples integer,
    center_name text,
    pmid text,
    journal text,
    citation_count integer,
    first_published text,
    place_name text,
    short_label text,
    longitude double precision,
    latitude double precision,
    city text,
    district text,
    subdistrict text,
    county text,
    suburb text,
    neighbourhood text,
    state text,
    state_district text,
    territory text,
    metro_area text,
    postcode text,
    country_code text,
    country text,
    country_mismatch text,
    geocode_score text,
    country_code_iso2 text,
    center_names jsonb,
    place_type text,
    address_type text,
    is_single_cell boolean,
    single_cell_modality text
);


ALTER TABLE public.unified_metadata OWNER TO sramap;

--
-- Name: enriched_crosstab; Type: MATERIALIZED VIEW; Schema: public; Owner: sramap
--

CREATE MATERIALIZED VIEW public.enriched_crosstab AS
 WITH base AS MATERIALIZED (
         SELECT COALESCE(cn.common_name, um.dominant_scientific_name) AS organism,
            os.tissue,
            os.cell_type,
            os.disease,
            os.assay,
            os.development_stage,
            os.sample_type,
            os.sex,
            os.ethnicity,
            os.phenotype,
            os.genetic_modification
           FROM ((public.ontology_samples_v3 os
             LEFT JOIN public.unified_metadata um ON ((um.canonical_accession = os.study_accession)))
             LEFT JOIN ( SELECT common_names.scientific_name,
                    min(common_names.common_name) AS common_name
                   FROM public.common_names
                  GROUP BY common_names.scientific_name) cn ON ((cn.scientific_name = um.dominant_scientific_name)))
        ), pairs AS (
         SELECT 'organism'::text AS group_col,
            base.organism AS group_val,
            'tissue'::text AS breakdown_col,
            base.tissue AS breakdown_val
           FROM base
          WHERE ((base.organism IS NOT NULL) AND (base.organism <> ''::text) AND (base.tissue IS NOT NULL) AND (base.tissue <> ''::text))
        UNION ALL
         SELECT 'organism'::text AS group_col,
            base.organism AS group_val,
            'cell_type'::text AS breakdown_col,
            base.cell_type AS breakdown_val
           FROM base
          WHERE ((base.organism IS NOT NULL) AND (base.organism <> ''::text) AND (base.cell_type IS NOT NULL) AND (base.cell_type <> ''::text))
        UNION ALL
         SELECT 'organism'::text AS group_col,
            base.organism AS group_val,
            'disease'::text AS breakdown_col,
            base.disease AS breakdown_val
           FROM base
          WHERE ((base.organism IS NOT NULL) AND (base.organism <> ''::text) AND (base.disease IS NOT NULL) AND (base.disease <> ''::text))
        UNION ALL
         SELECT 'organism'::text AS group_col,
            base.organism AS group_val,
            'assay'::text AS breakdown_col,
            base.assay AS breakdown_val
           FROM base
          WHERE ((base.organism IS NOT NULL) AND (base.organism <> ''::text) AND (base.assay IS NOT NULL) AND (base.assay <> ''::text))
        UNION ALL
         SELECT 'organism'::text AS group_col,
            base.organism AS group_val,
            'development_stage'::text AS breakdown_col,
            base.development_stage AS breakdown_val
           FROM base
          WHERE ((base.organism IS NOT NULL) AND (base.organism <> ''::text) AND (base.development_stage IS NOT NULL) AND (base.development_stage <> ''::text))
        UNION ALL
         SELECT 'organism'::text AS group_col,
            base.organism AS group_val,
            'sample_type'::text AS breakdown_col,
            base.sample_type AS breakdown_val
           FROM base
          WHERE ((base.organism IS NOT NULL) AND (base.organism <> ''::text) AND (base.sample_type IS NOT NULL) AND (base.sample_type <> ''::text))
        UNION ALL
         SELECT 'organism'::text AS group_col,
            base.organism AS group_val,
            'sex'::text AS breakdown_col,
            base.sex AS breakdown_val
           FROM base
          WHERE ((base.organism IS NOT NULL) AND (base.organism <> ''::text) AND (base.sex IS NOT NULL) AND (base.sex <> ''::text))
        UNION ALL
         SELECT 'organism'::text AS group_col,
            base.organism AS group_val,
            'ethnicity'::text AS breakdown_col,
            base.ethnicity AS breakdown_val
           FROM base
          WHERE ((base.organism IS NOT NULL) AND (base.organism <> ''::text) AND (base.ethnicity IS NOT NULL) AND (base.ethnicity <> ''::text))
        UNION ALL
         SELECT 'organism'::text AS group_col,
            base.organism AS group_val,
            'phenotype'::text AS breakdown_col,
            base.phenotype AS breakdown_val
           FROM base
          WHERE ((base.organism IS NOT NULL) AND (base.organism <> ''::text) AND (base.phenotype IS NOT NULL) AND (base.phenotype <> ''::text))
        UNION ALL
         SELECT 'organism'::text AS group_col,
            base.organism AS group_val,
            'genetic_modification'::text AS breakdown_col,
            base.genetic_modification AS breakdown_val
           FROM base
          WHERE ((base.organism IS NOT NULL) AND (base.organism <> ''::text) AND (base.genetic_modification IS NOT NULL) AND (base.genetic_modification <> ''::text))
        UNION ALL
         SELECT 'tissue'::text AS group_col,
            base.tissue AS group_val,
            'organism'::text AS breakdown_col,
            base.organism AS breakdown_val
           FROM base
          WHERE ((base.tissue IS NOT NULL) AND (base.tissue <> ''::text) AND (base.organism IS NOT NULL) AND (base.organism <> ''::text))
        UNION ALL
         SELECT 'tissue'::text AS group_col,
            base.tissue AS group_val,
            'cell_type'::text AS breakdown_col,
            base.cell_type AS breakdown_val
           FROM base
          WHERE ((base.tissue IS NOT NULL) AND (base.tissue <> ''::text) AND (base.cell_type IS NOT NULL) AND (base.cell_type <> ''::text))
        UNION ALL
         SELECT 'tissue'::text AS group_col,
            base.tissue AS group_val,
            'disease'::text AS breakdown_col,
            base.disease AS breakdown_val
           FROM base
          WHERE ((base.tissue IS NOT NULL) AND (base.tissue <> ''::text) AND (base.disease IS NOT NULL) AND (base.disease <> ''::text))
        UNION ALL
         SELECT 'tissue'::text AS group_col,
            base.tissue AS group_val,
            'assay'::text AS breakdown_col,
            base.assay AS breakdown_val
           FROM base
          WHERE ((base.tissue IS NOT NULL) AND (base.tissue <> ''::text) AND (base.assay IS NOT NULL) AND (base.assay <> ''::text))
        UNION ALL
         SELECT 'tissue'::text AS group_col,
            base.tissue AS group_val,
            'development_stage'::text AS breakdown_col,
            base.development_stage AS breakdown_val
           FROM base
          WHERE ((base.tissue IS NOT NULL) AND (base.tissue <> ''::text) AND (base.development_stage IS NOT NULL) AND (base.development_stage <> ''::text))
        UNION ALL
         SELECT 'tissue'::text AS group_col,
            base.tissue AS group_val,
            'sample_type'::text AS breakdown_col,
            base.sample_type AS breakdown_val
           FROM base
          WHERE ((base.tissue IS NOT NULL) AND (base.tissue <> ''::text) AND (base.sample_type IS NOT NULL) AND (base.sample_type <> ''::text))
        UNION ALL
         SELECT 'tissue'::text AS group_col,
            base.tissue AS group_val,
            'sex'::text AS breakdown_col,
            base.sex AS breakdown_val
           FROM base
          WHERE ((base.tissue IS NOT NULL) AND (base.tissue <> ''::text) AND (base.sex IS NOT NULL) AND (base.sex <> ''::text))
        UNION ALL
         SELECT 'tissue'::text AS group_col,
            base.tissue AS group_val,
            'ethnicity'::text AS breakdown_col,
            base.ethnicity AS breakdown_val
           FROM base
          WHERE ((base.tissue IS NOT NULL) AND (base.tissue <> ''::text) AND (base.ethnicity IS NOT NULL) AND (base.ethnicity <> ''::text))
        UNION ALL
         SELECT 'tissue'::text AS group_col,
            base.tissue AS group_val,
            'phenotype'::text AS breakdown_col,
            base.phenotype AS breakdown_val
           FROM base
          WHERE ((base.tissue IS NOT NULL) AND (base.tissue <> ''::text) AND (base.phenotype IS NOT NULL) AND (base.phenotype <> ''::text))
        UNION ALL
         SELECT 'tissue'::text AS group_col,
            base.tissue AS group_val,
            'genetic_modification'::text AS breakdown_col,
            base.genetic_modification AS breakdown_val
           FROM base
          WHERE ((base.tissue IS NOT NULL) AND (base.tissue <> ''::text) AND (base.genetic_modification IS NOT NULL) AND (base.genetic_modification <> ''::text))
        UNION ALL
         SELECT 'cell_type'::text AS group_col,
            base.cell_type AS group_val,
            'organism'::text AS breakdown_col,
            base.organism AS breakdown_val
           FROM base
          WHERE ((base.cell_type IS NOT NULL) AND (base.cell_type <> ''::text) AND (base.organism IS NOT NULL) AND (base.organism <> ''::text))
        UNION ALL
         SELECT 'cell_type'::text AS group_col,
            base.cell_type AS group_val,
            'tissue'::text AS breakdown_col,
            base.tissue AS breakdown_val
           FROM base
          WHERE ((base.cell_type IS NOT NULL) AND (base.cell_type <> ''::text) AND (base.tissue IS NOT NULL) AND (base.tissue <> ''::text))
        UNION ALL
         SELECT 'cell_type'::text AS group_col,
            base.cell_type AS group_val,
            'disease'::text AS breakdown_col,
            base.disease AS breakdown_val
           FROM base
          WHERE ((base.cell_type IS NOT NULL) AND (base.cell_type <> ''::text) AND (base.disease IS NOT NULL) AND (base.disease <> ''::text))
        UNION ALL
         SELECT 'cell_type'::text AS group_col,
            base.cell_type AS group_val,
            'assay'::text AS breakdown_col,
            base.assay AS breakdown_val
           FROM base
          WHERE ((base.cell_type IS NOT NULL) AND (base.cell_type <> ''::text) AND (base.assay IS NOT NULL) AND (base.assay <> ''::text))
        UNION ALL
         SELECT 'cell_type'::text AS group_col,
            base.cell_type AS group_val,
            'development_stage'::text AS breakdown_col,
            base.development_stage AS breakdown_val
           FROM base
          WHERE ((base.cell_type IS NOT NULL) AND (base.cell_type <> ''::text) AND (base.development_stage IS NOT NULL) AND (base.development_stage <> ''::text))
        UNION ALL
         SELECT 'cell_type'::text AS group_col,
            base.cell_type AS group_val,
            'sample_type'::text AS breakdown_col,
            base.sample_type AS breakdown_val
           FROM base
          WHERE ((base.cell_type IS NOT NULL) AND (base.cell_type <> ''::text) AND (base.sample_type IS NOT NULL) AND (base.sample_type <> ''::text))
        UNION ALL
         SELECT 'cell_type'::text AS group_col,
            base.cell_type AS group_val,
            'sex'::text AS breakdown_col,
            base.sex AS breakdown_val
           FROM base
          WHERE ((base.cell_type IS NOT NULL) AND (base.cell_type <> ''::text) AND (base.sex IS NOT NULL) AND (base.sex <> ''::text))
        UNION ALL
         SELECT 'cell_type'::text AS group_col,
            base.cell_type AS group_val,
            'ethnicity'::text AS breakdown_col,
            base.ethnicity AS breakdown_val
           FROM base
          WHERE ((base.cell_type IS NOT NULL) AND (base.cell_type <> ''::text) AND (base.ethnicity IS NOT NULL) AND (base.ethnicity <> ''::text))
        UNION ALL
         SELECT 'cell_type'::text AS group_col,
            base.cell_type AS group_val,
            'phenotype'::text AS breakdown_col,
            base.phenotype AS breakdown_val
           FROM base
          WHERE ((base.cell_type IS NOT NULL) AND (base.cell_type <> ''::text) AND (base.phenotype IS NOT NULL) AND (base.phenotype <> ''::text))
        UNION ALL
         SELECT 'cell_type'::text AS group_col,
            base.cell_type AS group_val,
            'genetic_modification'::text AS breakdown_col,
            base.genetic_modification AS breakdown_val
           FROM base
          WHERE ((base.cell_type IS NOT NULL) AND (base.cell_type <> ''::text) AND (base.genetic_modification IS NOT NULL) AND (base.genetic_modification <> ''::text))
        UNION ALL
         SELECT 'disease'::text AS group_col,
            base.disease AS group_val,
            'organism'::text AS breakdown_col,
            base.organism AS breakdown_val
           FROM base
          WHERE ((base.disease IS NOT NULL) AND (base.disease <> ''::text) AND (base.organism IS NOT NULL) AND (base.organism <> ''::text))
        UNION ALL
         SELECT 'disease'::text AS group_col,
            base.disease AS group_val,
            'tissue'::text AS breakdown_col,
            base.tissue AS breakdown_val
           FROM base
          WHERE ((base.disease IS NOT NULL) AND (base.disease <> ''::text) AND (base.tissue IS NOT NULL) AND (base.tissue <> ''::text))
        UNION ALL
         SELECT 'disease'::text AS group_col,
            base.disease AS group_val,
            'cell_type'::text AS breakdown_col,
            base.cell_type AS breakdown_val
           FROM base
          WHERE ((base.disease IS NOT NULL) AND (base.disease <> ''::text) AND (base.cell_type IS NOT NULL) AND (base.cell_type <> ''::text))
        UNION ALL
         SELECT 'disease'::text AS group_col,
            base.disease AS group_val,
            'assay'::text AS breakdown_col,
            base.assay AS breakdown_val
           FROM base
          WHERE ((base.disease IS NOT NULL) AND (base.disease <> ''::text) AND (base.assay IS NOT NULL) AND (base.assay <> ''::text))
        UNION ALL
         SELECT 'disease'::text AS group_col,
            base.disease AS group_val,
            'development_stage'::text AS breakdown_col,
            base.development_stage AS breakdown_val
           FROM base
          WHERE ((base.disease IS NOT NULL) AND (base.disease <> ''::text) AND (base.development_stage IS NOT NULL) AND (base.development_stage <> ''::text))
        UNION ALL
         SELECT 'disease'::text AS group_col,
            base.disease AS group_val,
            'sample_type'::text AS breakdown_col,
            base.sample_type AS breakdown_val
           FROM base
          WHERE ((base.disease IS NOT NULL) AND (base.disease <> ''::text) AND (base.sample_type IS NOT NULL) AND (base.sample_type <> ''::text))
        UNION ALL
         SELECT 'disease'::text AS group_col,
            base.disease AS group_val,
            'sex'::text AS breakdown_col,
            base.sex AS breakdown_val
           FROM base
          WHERE ((base.disease IS NOT NULL) AND (base.disease <> ''::text) AND (base.sex IS NOT NULL) AND (base.sex <> ''::text))
        UNION ALL
         SELECT 'disease'::text AS group_col,
            base.disease AS group_val,
            'ethnicity'::text AS breakdown_col,
            base.ethnicity AS breakdown_val
           FROM base
          WHERE ((base.disease IS NOT NULL) AND (base.disease <> ''::text) AND (base.ethnicity IS NOT NULL) AND (base.ethnicity <> ''::text))
        UNION ALL
         SELECT 'disease'::text AS group_col,
            base.disease AS group_val,
            'phenotype'::text AS breakdown_col,
            base.phenotype AS breakdown_val
           FROM base
          WHERE ((base.disease IS NOT NULL) AND (base.disease <> ''::text) AND (base.phenotype IS NOT NULL) AND (base.phenotype <> ''::text))
        UNION ALL
         SELECT 'disease'::text AS group_col,
            base.disease AS group_val,
            'genetic_modification'::text AS breakdown_col,
            base.genetic_modification AS breakdown_val
           FROM base
          WHERE ((base.disease IS NOT NULL) AND (base.disease <> ''::text) AND (base.genetic_modification IS NOT NULL) AND (base.genetic_modification <> ''::text))
        UNION ALL
         SELECT 'assay'::text AS group_col,
            base.assay AS group_val,
            'organism'::text AS breakdown_col,
            base.organism AS breakdown_val
           FROM base
          WHERE ((base.assay IS NOT NULL) AND (base.assay <> ''::text) AND (base.organism IS NOT NULL) AND (base.organism <> ''::text))
        UNION ALL
         SELECT 'assay'::text AS group_col,
            base.assay AS group_val,
            'tissue'::text AS breakdown_col,
            base.tissue AS breakdown_val
           FROM base
          WHERE ((base.assay IS NOT NULL) AND (base.assay <> ''::text) AND (base.tissue IS NOT NULL) AND (base.tissue <> ''::text))
        UNION ALL
         SELECT 'assay'::text AS group_col,
            base.assay AS group_val,
            'cell_type'::text AS breakdown_col,
            base.cell_type AS breakdown_val
           FROM base
          WHERE ((base.assay IS NOT NULL) AND (base.assay <> ''::text) AND (base.cell_type IS NOT NULL) AND (base.cell_type <> ''::text))
        UNION ALL
         SELECT 'assay'::text AS group_col,
            base.assay AS group_val,
            'disease'::text AS breakdown_col,
            base.disease AS breakdown_val
           FROM base
          WHERE ((base.assay IS NOT NULL) AND (base.assay <> ''::text) AND (base.disease IS NOT NULL) AND (base.disease <> ''::text))
        UNION ALL
         SELECT 'assay'::text AS group_col,
            base.assay AS group_val,
            'development_stage'::text AS breakdown_col,
            base.development_stage AS breakdown_val
           FROM base
          WHERE ((base.assay IS NOT NULL) AND (base.assay <> ''::text) AND (base.development_stage IS NOT NULL) AND (base.development_stage <> ''::text))
        UNION ALL
         SELECT 'assay'::text AS group_col,
            base.assay AS group_val,
            'sample_type'::text AS breakdown_col,
            base.sample_type AS breakdown_val
           FROM base
          WHERE ((base.assay IS NOT NULL) AND (base.assay <> ''::text) AND (base.sample_type IS NOT NULL) AND (base.sample_type <> ''::text))
        UNION ALL
         SELECT 'assay'::text AS group_col,
            base.assay AS group_val,
            'sex'::text AS breakdown_col,
            base.sex AS breakdown_val
           FROM base
          WHERE ((base.assay IS NOT NULL) AND (base.assay <> ''::text) AND (base.sex IS NOT NULL) AND (base.sex <> ''::text))
        UNION ALL
         SELECT 'assay'::text AS group_col,
            base.assay AS group_val,
            'ethnicity'::text AS breakdown_col,
            base.ethnicity AS breakdown_val
           FROM base
          WHERE ((base.assay IS NOT NULL) AND (base.assay <> ''::text) AND (base.ethnicity IS NOT NULL) AND (base.ethnicity <> ''::text))
        UNION ALL
         SELECT 'assay'::text AS group_col,
            base.assay AS group_val,
            'phenotype'::text AS breakdown_col,
            base.phenotype AS breakdown_val
           FROM base
          WHERE ((base.assay IS NOT NULL) AND (base.assay <> ''::text) AND (base.phenotype IS NOT NULL) AND (base.phenotype <> ''::text))
        UNION ALL
         SELECT 'assay'::text AS group_col,
            base.assay AS group_val,
            'genetic_modification'::text AS breakdown_col,
            base.genetic_modification AS breakdown_val
           FROM base
          WHERE ((base.assay IS NOT NULL) AND (base.assay <> ''::text) AND (base.genetic_modification IS NOT NULL) AND (base.genetic_modification <> ''::text))
        UNION ALL
         SELECT 'development_stage'::text AS group_col,
            base.development_stage AS group_val,
            'organism'::text AS breakdown_col,
            base.organism AS breakdown_val
           FROM base
          WHERE ((base.development_stage IS NOT NULL) AND (base.development_stage <> ''::text) AND (base.organism IS NOT NULL) AND (base.organism <> ''::text))
        UNION ALL
         SELECT 'development_stage'::text AS group_col,
            base.development_stage AS group_val,
            'tissue'::text AS breakdown_col,
            base.tissue AS breakdown_val
           FROM base
          WHERE ((base.development_stage IS NOT NULL) AND (base.development_stage <> ''::text) AND (base.tissue IS NOT NULL) AND (base.tissue <> ''::text))
        UNION ALL
         SELECT 'development_stage'::text AS group_col,
            base.development_stage AS group_val,
            'cell_type'::text AS breakdown_col,
            base.cell_type AS breakdown_val
           FROM base
          WHERE ((base.development_stage IS NOT NULL) AND (base.development_stage <> ''::text) AND (base.cell_type IS NOT NULL) AND (base.cell_type <> ''::text))
        UNION ALL
         SELECT 'development_stage'::text AS group_col,
            base.development_stage AS group_val,
            'disease'::text AS breakdown_col,
            base.disease AS breakdown_val
           FROM base
          WHERE ((base.development_stage IS NOT NULL) AND (base.development_stage <> ''::text) AND (base.disease IS NOT NULL) AND (base.disease <> ''::text))
        UNION ALL
         SELECT 'development_stage'::text AS group_col,
            base.development_stage AS group_val,
            'assay'::text AS breakdown_col,
            base.assay AS breakdown_val
           FROM base
          WHERE ((base.development_stage IS NOT NULL) AND (base.development_stage <> ''::text) AND (base.assay IS NOT NULL) AND (base.assay <> ''::text))
        UNION ALL
         SELECT 'development_stage'::text AS group_col,
            base.development_stage AS group_val,
            'sample_type'::text AS breakdown_col,
            base.sample_type AS breakdown_val
           FROM base
          WHERE ((base.development_stage IS NOT NULL) AND (base.development_stage <> ''::text) AND (base.sample_type IS NOT NULL) AND (base.sample_type <> ''::text))
        UNION ALL
         SELECT 'development_stage'::text AS group_col,
            base.development_stage AS group_val,
            'sex'::text AS breakdown_col,
            base.sex AS breakdown_val
           FROM base
          WHERE ((base.development_stage IS NOT NULL) AND (base.development_stage <> ''::text) AND (base.sex IS NOT NULL) AND (base.sex <> ''::text))
        UNION ALL
         SELECT 'development_stage'::text AS group_col,
            base.development_stage AS group_val,
            'ethnicity'::text AS breakdown_col,
            base.ethnicity AS breakdown_val
           FROM base
          WHERE ((base.development_stage IS NOT NULL) AND (base.development_stage <> ''::text) AND (base.ethnicity IS NOT NULL) AND (base.ethnicity <> ''::text))
        UNION ALL
         SELECT 'development_stage'::text AS group_col,
            base.development_stage AS group_val,
            'phenotype'::text AS breakdown_col,
            base.phenotype AS breakdown_val
           FROM base
          WHERE ((base.development_stage IS NOT NULL) AND (base.development_stage <> ''::text) AND (base.phenotype IS NOT NULL) AND (base.phenotype <> ''::text))
        UNION ALL
         SELECT 'development_stage'::text AS group_col,
            base.development_stage AS group_val,
            'genetic_modification'::text AS breakdown_col,
            base.genetic_modification AS breakdown_val
           FROM base
          WHERE ((base.development_stage IS NOT NULL) AND (base.development_stage <> ''::text) AND (base.genetic_modification IS NOT NULL) AND (base.genetic_modification <> ''::text))
        UNION ALL
         SELECT 'sample_type'::text AS group_col,
            base.sample_type AS group_val,
            'organism'::text AS breakdown_col,
            base.organism AS breakdown_val
           FROM base
          WHERE ((base.sample_type IS NOT NULL) AND (base.sample_type <> ''::text) AND (base.organism IS NOT NULL) AND (base.organism <> ''::text))
        UNION ALL
         SELECT 'sample_type'::text AS group_col,
            base.sample_type AS group_val,
            'tissue'::text AS breakdown_col,
            base.tissue AS breakdown_val
           FROM base
          WHERE ((base.sample_type IS NOT NULL) AND (base.sample_type <> ''::text) AND (base.tissue IS NOT NULL) AND (base.tissue <> ''::text))
        UNION ALL
         SELECT 'sample_type'::text AS group_col,
            base.sample_type AS group_val,
            'cell_type'::text AS breakdown_col,
            base.cell_type AS breakdown_val
           FROM base
          WHERE ((base.sample_type IS NOT NULL) AND (base.sample_type <> ''::text) AND (base.cell_type IS NOT NULL) AND (base.cell_type <> ''::text))
        UNION ALL
         SELECT 'sample_type'::text AS group_col,
            base.sample_type AS group_val,
            'disease'::text AS breakdown_col,
            base.disease AS breakdown_val
           FROM base
          WHERE ((base.sample_type IS NOT NULL) AND (base.sample_type <> ''::text) AND (base.disease IS NOT NULL) AND (base.disease <> ''::text))
        UNION ALL
         SELECT 'sample_type'::text AS group_col,
            base.sample_type AS group_val,
            'assay'::text AS breakdown_col,
            base.assay AS breakdown_val
           FROM base
          WHERE ((base.sample_type IS NOT NULL) AND (base.sample_type <> ''::text) AND (base.assay IS NOT NULL) AND (base.assay <> ''::text))
        UNION ALL
         SELECT 'sample_type'::text AS group_col,
            base.sample_type AS group_val,
            'development_stage'::text AS breakdown_col,
            base.development_stage AS breakdown_val
           FROM base
          WHERE ((base.sample_type IS NOT NULL) AND (base.sample_type <> ''::text) AND (base.development_stage IS NOT NULL) AND (base.development_stage <> ''::text))
        UNION ALL
         SELECT 'sample_type'::text AS group_col,
            base.sample_type AS group_val,
            'sex'::text AS breakdown_col,
            base.sex AS breakdown_val
           FROM base
          WHERE ((base.sample_type IS NOT NULL) AND (base.sample_type <> ''::text) AND (base.sex IS NOT NULL) AND (base.sex <> ''::text))
        UNION ALL
         SELECT 'sample_type'::text AS group_col,
            base.sample_type AS group_val,
            'ethnicity'::text AS breakdown_col,
            base.ethnicity AS breakdown_val
           FROM base
          WHERE ((base.sample_type IS NOT NULL) AND (base.sample_type <> ''::text) AND (base.ethnicity IS NOT NULL) AND (base.ethnicity <> ''::text))
        UNION ALL
         SELECT 'sample_type'::text AS group_col,
            base.sample_type AS group_val,
            'phenotype'::text AS breakdown_col,
            base.phenotype AS breakdown_val
           FROM base
          WHERE ((base.sample_type IS NOT NULL) AND (base.sample_type <> ''::text) AND (base.phenotype IS NOT NULL) AND (base.phenotype <> ''::text))
        UNION ALL
         SELECT 'sample_type'::text AS group_col,
            base.sample_type AS group_val,
            'genetic_modification'::text AS breakdown_col,
            base.genetic_modification AS breakdown_val
           FROM base
          WHERE ((base.sample_type IS NOT NULL) AND (base.sample_type <> ''::text) AND (base.genetic_modification IS NOT NULL) AND (base.genetic_modification <> ''::text))
        UNION ALL
         SELECT 'sex'::text AS group_col,
            base.sex AS group_val,
            'organism'::text AS breakdown_col,
            base.organism AS breakdown_val
           FROM base
          WHERE ((base.sex IS NOT NULL) AND (base.sex <> ''::text) AND (base.organism IS NOT NULL) AND (base.organism <> ''::text))
        UNION ALL
         SELECT 'sex'::text AS group_col,
            base.sex AS group_val,
            'tissue'::text AS breakdown_col,
            base.tissue AS breakdown_val
           FROM base
          WHERE ((base.sex IS NOT NULL) AND (base.sex <> ''::text) AND (base.tissue IS NOT NULL) AND (base.tissue <> ''::text))
        UNION ALL
         SELECT 'sex'::text AS group_col,
            base.sex AS group_val,
            'cell_type'::text AS breakdown_col,
            base.cell_type AS breakdown_val
           FROM base
          WHERE ((base.sex IS NOT NULL) AND (base.sex <> ''::text) AND (base.cell_type IS NOT NULL) AND (base.cell_type <> ''::text))
        UNION ALL
         SELECT 'sex'::text AS group_col,
            base.sex AS group_val,
            'disease'::text AS breakdown_col,
            base.disease AS breakdown_val
           FROM base
          WHERE ((base.sex IS NOT NULL) AND (base.sex <> ''::text) AND (base.disease IS NOT NULL) AND (base.disease <> ''::text))
        UNION ALL
         SELECT 'sex'::text AS group_col,
            base.sex AS group_val,
            'assay'::text AS breakdown_col,
            base.assay AS breakdown_val
           FROM base
          WHERE ((base.sex IS NOT NULL) AND (base.sex <> ''::text) AND (base.assay IS NOT NULL) AND (base.assay <> ''::text))
        UNION ALL
         SELECT 'sex'::text AS group_col,
            base.sex AS group_val,
            'development_stage'::text AS breakdown_col,
            base.development_stage AS breakdown_val
           FROM base
          WHERE ((base.sex IS NOT NULL) AND (base.sex <> ''::text) AND (base.development_stage IS NOT NULL) AND (base.development_stage <> ''::text))
        UNION ALL
         SELECT 'sex'::text AS group_col,
            base.sex AS group_val,
            'sample_type'::text AS breakdown_col,
            base.sample_type AS breakdown_val
           FROM base
          WHERE ((base.sex IS NOT NULL) AND (base.sex <> ''::text) AND (base.sample_type IS NOT NULL) AND (base.sample_type <> ''::text))
        UNION ALL
         SELECT 'sex'::text AS group_col,
            base.sex AS group_val,
            'ethnicity'::text AS breakdown_col,
            base.ethnicity AS breakdown_val
           FROM base
          WHERE ((base.sex IS NOT NULL) AND (base.sex <> ''::text) AND (base.ethnicity IS NOT NULL) AND (base.ethnicity <> ''::text))
        UNION ALL
         SELECT 'sex'::text AS group_col,
            base.sex AS group_val,
            'phenotype'::text AS breakdown_col,
            base.phenotype AS breakdown_val
           FROM base
          WHERE ((base.sex IS NOT NULL) AND (base.sex <> ''::text) AND (base.phenotype IS NOT NULL) AND (base.phenotype <> ''::text))
        UNION ALL
         SELECT 'sex'::text AS group_col,
            base.sex AS group_val,
            'genetic_modification'::text AS breakdown_col,
            base.genetic_modification AS breakdown_val
           FROM base
          WHERE ((base.sex IS NOT NULL) AND (base.sex <> ''::text) AND (base.genetic_modification IS NOT NULL) AND (base.genetic_modification <> ''::text))
        UNION ALL
         SELECT 'ethnicity'::text AS group_col,
            base.ethnicity AS group_val,
            'organism'::text AS breakdown_col,
            base.organism AS breakdown_val
           FROM base
          WHERE ((base.ethnicity IS NOT NULL) AND (base.ethnicity <> ''::text) AND (base.organism IS NOT NULL) AND (base.organism <> ''::text))
        UNION ALL
         SELECT 'ethnicity'::text AS group_col,
            base.ethnicity AS group_val,
            'tissue'::text AS breakdown_col,
            base.tissue AS breakdown_val
           FROM base
          WHERE ((base.ethnicity IS NOT NULL) AND (base.ethnicity <> ''::text) AND (base.tissue IS NOT NULL) AND (base.tissue <> ''::text))
        UNION ALL
         SELECT 'ethnicity'::text AS group_col,
            base.ethnicity AS group_val,
            'cell_type'::text AS breakdown_col,
            base.cell_type AS breakdown_val
           FROM base
          WHERE ((base.ethnicity IS NOT NULL) AND (base.ethnicity <> ''::text) AND (base.cell_type IS NOT NULL) AND (base.cell_type <> ''::text))
        UNION ALL
         SELECT 'ethnicity'::text AS group_col,
            base.ethnicity AS group_val,
            'disease'::text AS breakdown_col,
            base.disease AS breakdown_val
           FROM base
          WHERE ((base.ethnicity IS NOT NULL) AND (base.ethnicity <> ''::text) AND (base.disease IS NOT NULL) AND (base.disease <> ''::text))
        UNION ALL
         SELECT 'ethnicity'::text AS group_col,
            base.ethnicity AS group_val,
            'assay'::text AS breakdown_col,
            base.assay AS breakdown_val
           FROM base
          WHERE ((base.ethnicity IS NOT NULL) AND (base.ethnicity <> ''::text) AND (base.assay IS NOT NULL) AND (base.assay <> ''::text))
        UNION ALL
         SELECT 'ethnicity'::text AS group_col,
            base.ethnicity AS group_val,
            'development_stage'::text AS breakdown_col,
            base.development_stage AS breakdown_val
           FROM base
          WHERE ((base.ethnicity IS NOT NULL) AND (base.ethnicity <> ''::text) AND (base.development_stage IS NOT NULL) AND (base.development_stage <> ''::text))
        UNION ALL
         SELECT 'ethnicity'::text AS group_col,
            base.ethnicity AS group_val,
            'sample_type'::text AS breakdown_col,
            base.sample_type AS breakdown_val
           FROM base
          WHERE ((base.ethnicity IS NOT NULL) AND (base.ethnicity <> ''::text) AND (base.sample_type IS NOT NULL) AND (base.sample_type <> ''::text))
        UNION ALL
         SELECT 'ethnicity'::text AS group_col,
            base.ethnicity AS group_val,
            'sex'::text AS breakdown_col,
            base.sex AS breakdown_val
           FROM base
          WHERE ((base.ethnicity IS NOT NULL) AND (base.ethnicity <> ''::text) AND (base.sex IS NOT NULL) AND (base.sex <> ''::text))
        UNION ALL
         SELECT 'ethnicity'::text AS group_col,
            base.ethnicity AS group_val,
            'phenotype'::text AS breakdown_col,
            base.phenotype AS breakdown_val
           FROM base
          WHERE ((base.ethnicity IS NOT NULL) AND (base.ethnicity <> ''::text) AND (base.phenotype IS NOT NULL) AND (base.phenotype <> ''::text))
        UNION ALL
         SELECT 'ethnicity'::text AS group_col,
            base.ethnicity AS group_val,
            'genetic_modification'::text AS breakdown_col,
            base.genetic_modification AS breakdown_val
           FROM base
          WHERE ((base.ethnicity IS NOT NULL) AND (base.ethnicity <> ''::text) AND (base.genetic_modification IS NOT NULL) AND (base.genetic_modification <> ''::text))
        UNION ALL
         SELECT 'phenotype'::text AS group_col,
            base.phenotype AS group_val,
            'organism'::text AS breakdown_col,
            base.organism AS breakdown_val
           FROM base
          WHERE ((base.phenotype IS NOT NULL) AND (base.phenotype <> ''::text) AND (base.organism IS NOT NULL) AND (base.organism <> ''::text))
        UNION ALL
         SELECT 'phenotype'::text AS group_col,
            base.phenotype AS group_val,
            'tissue'::text AS breakdown_col,
            base.tissue AS breakdown_val
           FROM base
          WHERE ((base.phenotype IS NOT NULL) AND (base.phenotype <> ''::text) AND (base.tissue IS NOT NULL) AND (base.tissue <> ''::text))
        UNION ALL
         SELECT 'phenotype'::text AS group_col,
            base.phenotype AS group_val,
            'cell_type'::text AS breakdown_col,
            base.cell_type AS breakdown_val
           FROM base
          WHERE ((base.phenotype IS NOT NULL) AND (base.phenotype <> ''::text) AND (base.cell_type IS NOT NULL) AND (base.cell_type <> ''::text))
        UNION ALL
         SELECT 'phenotype'::text AS group_col,
            base.phenotype AS group_val,
            'disease'::text AS breakdown_col,
            base.disease AS breakdown_val
           FROM base
          WHERE ((base.phenotype IS NOT NULL) AND (base.phenotype <> ''::text) AND (base.disease IS NOT NULL) AND (base.disease <> ''::text))
        UNION ALL
         SELECT 'phenotype'::text AS group_col,
            base.phenotype AS group_val,
            'assay'::text AS breakdown_col,
            base.assay AS breakdown_val
           FROM base
          WHERE ((base.phenotype IS NOT NULL) AND (base.phenotype <> ''::text) AND (base.assay IS NOT NULL) AND (base.assay <> ''::text))
        UNION ALL
         SELECT 'phenotype'::text AS group_col,
            base.phenotype AS group_val,
            'development_stage'::text AS breakdown_col,
            base.development_stage AS breakdown_val
           FROM base
          WHERE ((base.phenotype IS NOT NULL) AND (base.phenotype <> ''::text) AND (base.development_stage IS NOT NULL) AND (base.development_stage <> ''::text))
        UNION ALL
         SELECT 'phenotype'::text AS group_col,
            base.phenotype AS group_val,
            'sample_type'::text AS breakdown_col,
            base.sample_type AS breakdown_val
           FROM base
          WHERE ((base.phenotype IS NOT NULL) AND (base.phenotype <> ''::text) AND (base.sample_type IS NOT NULL) AND (base.sample_type <> ''::text))
        UNION ALL
         SELECT 'phenotype'::text AS group_col,
            base.phenotype AS group_val,
            'sex'::text AS breakdown_col,
            base.sex AS breakdown_val
           FROM base
          WHERE ((base.phenotype IS NOT NULL) AND (base.phenotype <> ''::text) AND (base.sex IS NOT NULL) AND (base.sex <> ''::text))
        UNION ALL
         SELECT 'phenotype'::text AS group_col,
            base.phenotype AS group_val,
            'ethnicity'::text AS breakdown_col,
            base.ethnicity AS breakdown_val
           FROM base
          WHERE ((base.phenotype IS NOT NULL) AND (base.phenotype <> ''::text) AND (base.ethnicity IS NOT NULL) AND (base.ethnicity <> ''::text))
        UNION ALL
         SELECT 'phenotype'::text AS group_col,
            base.phenotype AS group_val,
            'genetic_modification'::text AS breakdown_col,
            base.genetic_modification AS breakdown_val
           FROM base
          WHERE ((base.phenotype IS NOT NULL) AND (base.phenotype <> ''::text) AND (base.genetic_modification IS NOT NULL) AND (base.genetic_modification <> ''::text))
        UNION ALL
         SELECT 'genetic_modification'::text AS group_col,
            base.genetic_modification AS group_val,
            'organism'::text AS breakdown_col,
            base.organism AS breakdown_val
           FROM base
          WHERE ((base.genetic_modification IS NOT NULL) AND (base.genetic_modification <> ''::text) AND (base.organism IS NOT NULL) AND (base.organism <> ''::text))
        UNION ALL
         SELECT 'genetic_modification'::text AS group_col,
            base.genetic_modification AS group_val,
            'tissue'::text AS breakdown_col,
            base.tissue AS breakdown_val
           FROM base
          WHERE ((base.genetic_modification IS NOT NULL) AND (base.genetic_modification <> ''::text) AND (base.tissue IS NOT NULL) AND (base.tissue <> ''::text))
        UNION ALL
         SELECT 'genetic_modification'::text AS group_col,
            base.genetic_modification AS group_val,
            'cell_type'::text AS breakdown_col,
            base.cell_type AS breakdown_val
           FROM base
          WHERE ((base.genetic_modification IS NOT NULL) AND (base.genetic_modification <> ''::text) AND (base.cell_type IS NOT NULL) AND (base.cell_type <> ''::text))
        UNION ALL
         SELECT 'genetic_modification'::text AS group_col,
            base.genetic_modification AS group_val,
            'disease'::text AS breakdown_col,
            base.disease AS breakdown_val
           FROM base
          WHERE ((base.genetic_modification IS NOT NULL) AND (base.genetic_modification <> ''::text) AND (base.disease IS NOT NULL) AND (base.disease <> ''::text))
        UNION ALL
         SELECT 'genetic_modification'::text AS group_col,
            base.genetic_modification AS group_val,
            'assay'::text AS breakdown_col,
            base.assay AS breakdown_val
           FROM base
          WHERE ((base.genetic_modification IS NOT NULL) AND (base.genetic_modification <> ''::text) AND (base.assay IS NOT NULL) AND (base.assay <> ''::text))
        UNION ALL
         SELECT 'genetic_modification'::text AS group_col,
            base.genetic_modification AS group_val,
            'development_stage'::text AS breakdown_col,
            base.development_stage AS breakdown_val
           FROM base
          WHERE ((base.genetic_modification IS NOT NULL) AND (base.genetic_modification <> ''::text) AND (base.development_stage IS NOT NULL) AND (base.development_stage <> ''::text))
        UNION ALL
         SELECT 'genetic_modification'::text AS group_col,
            base.genetic_modification AS group_val,
            'sample_type'::text AS breakdown_col,
            base.sample_type AS breakdown_val
           FROM base
          WHERE ((base.genetic_modification IS NOT NULL) AND (base.genetic_modification <> ''::text) AND (base.sample_type IS NOT NULL) AND (base.sample_type <> ''::text))
        UNION ALL
         SELECT 'genetic_modification'::text AS group_col,
            base.genetic_modification AS group_val,
            'sex'::text AS breakdown_col,
            base.sex AS breakdown_val
           FROM base
          WHERE ((base.genetic_modification IS NOT NULL) AND (base.genetic_modification <> ''::text) AND (base.sex IS NOT NULL) AND (base.sex <> ''::text))
        UNION ALL
         SELECT 'genetic_modification'::text AS group_col,
            base.genetic_modification AS group_val,
            'ethnicity'::text AS breakdown_col,
            base.ethnicity AS breakdown_val
           FROM base
          WHERE ((base.genetic_modification IS NOT NULL) AND (base.genetic_modification <> ''::text) AND (base.ethnicity IS NOT NULL) AND (base.ethnicity <> ''::text))
        UNION ALL
         SELECT 'genetic_modification'::text AS group_col,
            base.genetic_modification AS group_val,
            'phenotype'::text AS breakdown_col,
            base.phenotype AS breakdown_val
           FROM base
          WHERE ((base.genetic_modification IS NOT NULL) AND (base.genetic_modification <> ''::text) AND (base.phenotype IS NOT NULL) AND (base.phenotype <> ''::text))
        ), counts AS (
         SELECT pairs.group_col,
            pairs.group_val,
            pairs.breakdown_col,
            pairs.breakdown_val,
            count(*) AS n
           FROM pairs
          GROUP BY pairs.group_col, pairs.group_val, pairs.breakdown_col, pairs.breakdown_val
        ), grp AS (
         SELECT counts.group_col,
            counts.group_val,
            counts.breakdown_col,
            sum(counts.n) AS group_total,
            count(*) AS group_distinct
           FROM counts
          GROUP BY counts.group_col, counts.group_val, counts.breakdown_col
        ), ranked AS (
         SELECT c.group_col,
            c.group_val,
            c.breakdown_col,
            c.breakdown_val,
            c.n,
            g.group_total,
            g.group_distinct,
            row_number() OVER (PARTITION BY c.group_col, c.group_val, c.breakdown_col ORDER BY c.n DESC) AS brk,
            dense_rank() OVER (PARTITION BY c.group_col, c.breakdown_col ORDER BY g.group_total DESC, c.group_val) AS grk
           FROM (counts c
             JOIN grp g USING (group_col, group_val, breakdown_col))
        )
 SELECT group_col,
    group_val,
    breakdown_col,
    breakdown_val,
    n,
    group_total,
    group_distinct
   FROM ranked
  WHERE ((brk <= 50) AND (grk <= 100))
  WITH NO DATA;


ALTER MATERIALIZED VIEW public.enriched_crosstab OWNER TO sramap;

--
-- Name: enriched_samples; Type: TABLE; Schema: public; Owner: sramap
--

CREATE TABLE public.enriched_samples (
    study_accession text NOT NULL,
    sample text NOT NULL,
    age text,
    sex text,
    ethnicity text,
    phenotype text,
    cell_type text,
    tissue text,
    strain text,
    disease text,
    assay text,
    cell_line text,
    treatment text,
    development_stage text,
    sample_type text,
    genetic_modification text,
    cell_count integer,
    gene_count integer,
    cell_count_estimated boolean DEFAULT false
);


ALTER TABLE public.enriched_samples OWNER TO sramap;

--
-- Name: enriched_samples_v3; Type: TABLE; Schema: public; Owner: sramap
--

CREATE TABLE public.enriched_samples_v3 (
    study_accession text NOT NULL,
    sample text NOT NULL,
    age text,
    sex text,
    ethnicity text,
    phenotype text,
    cell_type text,
    tissue text,
    strain text,
    disease text,
    assay text,
    assay_category text,
    cell_line text,
    treatment text,
    development_stage text,
    sample_type text,
    genetic_modification text,
    cell_count integer,
    gene_count integer,
    cell_count_estimated boolean
);


ALTER TABLE public.enriched_samples_v3 OWNER TO sramap;

--
-- Name: enriched_studies; Type: TABLE; Schema: public; Owner: sramap
--

CREATE TABLE public.enriched_studies (
    accession text NOT NULL,
    title text,
    n_samples integer DEFAULT 0 NOT NULL,
    single_cell_modality text
);


ALTER TABLE public.enriched_studies OWNER TO sramap;

--
-- Name: enriched_studies_v3; Type: TABLE; Schema: public; Owner: sramap
--

CREATE TABLE public.enriched_studies_v3 (
    accession text NOT NULL,
    title text,
    n_samples integer DEFAULT 0 NOT NULL,
    single_cell_modality text
);


ALTER TABLE public.enriched_studies_v3 OWNER TO sramap;

--
-- Name: enriched_summary; Type: MATERIALIZED VIEW; Schema: public; Owner: sramap
--

CREATE MATERIALIZED VIEW public.enriched_summary AS
 SELECT count(*) AS enriched_samples,
    count(DISTINCT study_accession) AS enriched_projects,
    count(tissue) AS tissue_filled,
    count(DISTINCT tissue) AS tissue_distinct,
    count(cell_type) AS cell_type_filled,
    count(DISTINCT cell_type) AS cell_type_distinct,
    count(disease) AS disease_filled,
    count(DISTINCT disease) AS disease_distinct,
    count(assay) AS assay_filled,
    count(DISTINCT assay) AS assay_distinct,
    count(assay_category) AS assay_category_filled,
    count(DISTINCT assay_category) AS assay_category_distinct,
    count(sex) AS sex_filled,
    count(DISTINCT sex) AS sex_distinct,
    count(organism) AS organism_filled,
    count(DISTINCT organism) AS organism_distinct,
    count(development_stage) AS development_stage_filled,
    count(DISTINCT development_stage) AS development_stage_distinct,
    count(sample_type) AS sample_type_filled,
    count(DISTINCT sample_type) AS sample_type_distinct,
    count(strain) AS strain_filled,
    count(DISTINCT strain) AS strain_distinct,
    count(treatment) AS treatment_filled,
    count(DISTINCT treatment) AS treatment_distinct,
    count(cell_line) AS cell_line_filled,
    count(DISTINCT cell_line) AS cell_line_distinct,
    count(ethnicity) AS ethnicity_filled,
    count(DISTINCT ethnicity) AS ethnicity_distinct,
    count(phenotype) AS phenotype_filled,
    count(DISTINCT phenotype) AS phenotype_distinct,
    count(genetic_modification) AS genetic_modification_filled,
    count(DISTINCT genetic_modification) AS genetic_modification_distinct,
    count(tissue_primary_site) AS tissue_primary_site_filled,
    count(DISTINCT tissue_primary_site) AS tissue_primary_site_distinct,
    count(tissue_site_type) AS tissue_site_type_filled,
    count(DISTINCT tissue_site_type) AS tissue_site_type_distinct
   FROM public.ontology_samples_v3
  WITH NO DATA;


ALTER MATERIALIZED VIEW public.enriched_summary OWNER TO sramap;

--
-- Name: geo_contributors; Type: TABLE; Schema: public; Owner: sramap
--

CREATE TABLE public.geo_contributors (
    id bigint NOT NULL,
    accession text NOT NULL,
    person text,
    department text,
    organization text,
    address_line text,
    address_city text,
    address_state text,
    address_zipcode text,
    address_country text
);


ALTER TABLE public.geo_contributors OWNER TO sramap;

--
-- Name: geo_contributors_id_seq; Type: SEQUENCE; Schema: public; Owner: sramap
--

CREATE SEQUENCE public.geo_contributors_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.geo_contributors_id_seq OWNER TO sramap;

--
-- Name: geo_contributors_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: sramap
--

ALTER SEQUENCE public.geo_contributors_id_seq OWNED BY public.geo_contributors.id;


--
-- Name: geo_geocode; Type: TABLE; Schema: public; Owner: sramap
--

CREATE TABLE public.geo_geocode (
    organization text,
    department text,
    address_line text,
    address_city text,
    address_state text,
    address_zipcode text,
    address_country text,
    n_series integer,
    full_address text,
    address_key text NOT NULL,
    geo_query text,
    geo_formatted_address text,
    geo_place_name text,
    geo_place_type text,
    geo_address_type text,
    geo_longitude double precision,
    geo_latitude double precision,
    geo_city text,
    geo_district text,
    geo_subdistrict text,
    geo_county text,
    geo_suburb text,
    geo_neighbourhood text,
    geo_state text,
    geo_postcode text,
    geo_country text,
    geo_country_code text,
    geo_source text,
    geo_country_mismatch boolean,
    geo_error text,
    search_vector tsvector GENERATED ALWAYS AS (to_tsvector('english'::regconfig, ((((((COALESCE(organization, ''::text) || ' '::text) || COALESCE(department, ''::text)) || ' '::text) || COALESCE(geo_place_name, ''::text)) || ' '::text) || COALESCE(full_address, ''::text)))) STORED
);


ALTER TABLE public.geo_geocode OWNER TO sramap;

--
-- Name: geo_platforms; Type: TABLE; Schema: public; Owner: sramap
--

CREATE TABLE public.geo_platforms (
    accession text NOT NULL,
    description text,
    title text,
    technology text,
    published_at date,
    updated_at date,
    organism text,
    manufacturer text,
    relation text
);


ALTER TABLE public.geo_platforms OWNER TO sramap;

--
-- Name: geo_samples; Type: TABLE; Schema: public; Owner: sramap
--

CREATE TABLE public.geo_samples (
    accession text NOT NULL,
    channel_count integer,
    channels jsonb[],
    description text,
    platform_ref text,
    published_at date,
    updated_at date,
    supplementary_data jsonb[],
    title text,
    sample_type text,
    hybridization_protocol text,
    scan_protocol text
);


ALTER TABLE public.geo_samples OWNER TO sramap;

--
-- Name: geo_series; Type: TABLE; Schema: public; Owner: sramap
--

CREATE TABLE public.geo_series (
    accession text NOT NULL,
    overall_design text,
    summary text,
    title text,
    pubmed_id text[],
    samples_ref text[],
    series_type text[],
    relation jsonb[],
    supplementary_data jsonb[],
    published_at date,
    updated_at date,
    pubmed_id_pysradb text,
    pubmed_last_checked date,
    bioproject_id text,
    organisms text[],
    alias text,
    coords_2d double precision[],
    coords_3d double precision[],
    neighbors jsonb[],
    countries text[],
    contributors text[],
    departments text[],
    organizations text[],
    tsv tsvector,
    countries_iso text[],
    library_strategies text[],
    instrument_models text[],
    alias_all text[],
    platforms text[],
    clusters jsonb
);


ALTER TABLE public.geo_series OWNER TO sramap;

--
-- Name: low_confidence; Type: TABLE; Schema: public; Owner: sramap
--

CREATE TABLE public.low_confidence (
    study_accession text NOT NULL,
    sample_accession text NOT NULL,
    tags text[] DEFAULT '{}'::text[] NOT NULL
);


ALTER TABLE public.low_confidence OWNER TO sramap;

--
-- Name: sra_experiments; Type: TABLE; Schema: public; Owner: sramap
--

CREATE TABLE public.sra_experiments (
    accession text NOT NULL,
    design_description text,
    library_layout text,
    library_name text,
    library_selection text,
    library_source text,
    library_strategy text,
    samples text[],
    platform text,
    instrument_model text,
    study text,
    title text,
    submission text
);


ALTER TABLE public.sra_experiments OWNER TO sramap;

--
-- Name: sra_studies; Type: TABLE; Schema: public; Owner: sramap
--

CREATE TABLE public.sra_studies (
    accession text NOT NULL,
    alias text,
    title text,
    abstract text,
    submission text,
    study_type text,
    published_at date,
    updated_at date,
    external_id jsonb,
    attributes_json jsonb,
    links jsonb,
    center_project_name text,
    pubmed_id_pysradb text,
    pubmed_last_checked date,
    organisms text[],
    bioproject_id text,
    coords_2d double precision[],
    coords_3d double precision[],
    neighbors jsonb[],
    countries text[],
    tsv tsvector GENERATED ALWAYS AS ((((setweight(to_tsvector('english'::regconfig, COALESCE(title, ''::text)), 'A'::"char") || setweight(to_tsvector('english'::regconfig, COALESCE(abstract, ''::text)), 'B'::"char")) || setweight(to_tsvector('english'::regconfig, COALESCE(center_project_name, ''::text)), 'C'::"char")) || setweight(to_tsvector('english'::regconfig, COALESCE(study_type, ''::text)), 'D'::"char"))) STORED,
    library_strategies text[],
    instrument_models text[],
    platforms text[],
    clusters jsonb
);


ALTER TABLE public.sra_studies OWNER TO sramap;

--
-- Name: organism_growth_monthly; Type: MATERIALIZED VIEW; Schema: public; Owner: sramap
--

CREATE MATERIALIZED VIEW public.organism_growth_monthly AS
 SELECT 'geo'::text AS db,
    unnest(g.organisms) AS organism,
    date_trunc('month'::text, (s.published_at)::timestamp with time zone) AS month,
    count(*) AS count
   FROM (public.geo_samples s
     JOIN public.geo_series g ON ((s.accession = ANY (g.samples_ref))))
  WHERE ((s.published_at IS NOT NULL) AND (g.organisms IS NOT NULL))
  GROUP BY (unnest(g.organisms)), (date_trunc('month'::text, (s.published_at)::timestamp with time zone))
UNION ALL
 SELECT 'sra'::text AS db,
    unnest(st.organisms) AS organism,
    date_trunc('month'::text, (st.published_at)::timestamp with time zone) AS month,
    count(*) AS count
   FROM (public.sra_experiments e
     JOIN public.sra_studies st ON ((st.accession = e.study)))
  WHERE ((st.published_at IS NOT NULL) AND (st.organisms IS NOT NULL))
  GROUP BY (unnest(st.organisms)), (date_trunc('month'::text, (st.published_at)::timestamp with time zone))
UNION ALL
 SELECT 'arrayexpress'::text AS db,
    s.organism,
    date_trunc('month'::text, (ex.release_date)::timestamp with time zone) AS month,
    count(*) AS count
   FROM (public.arrayexpress_samples s
     JOIN public.arrayexpress_experiments ex ON ((ex.accession = s.experiment_accession)))
  WHERE ((ex.release_date IS NOT NULL) AND (s.organism IS NOT NULL) AND (s.organism <> ''::text))
  GROUP BY s.organism, (date_trunc('month'::text, (ex.release_date)::timestamp with time zone))
UNION ALL
 SELECT 'ena'::text AS db,
    es.scientific_name AS organism,
    date_trunc('month'::text, (e.first_public)::timestamp with time zone) AS month,
    count(*) AS count
   FROM (public.ena_experiments e
     JOIN public.ena_studies es ON ((es.study_accession = e.study_accession)))
  WHERE ((e.first_public IS NOT NULL) AND (es.scientific_name IS NOT NULL))
  GROUP BY es.scientific_name, (date_trunc('month'::text, (e.first_public)::timestamp with time zone))
  WITH NO DATA;


ALTER MATERIALIZED VIEW public.organism_growth_monthly OWNER TO sramap;

--
-- Name: platform_growth_monthly; Type: MATERIALIZED VIEW; Schema: public; Owner: sramap
--

CREATE MATERIALIZED VIEW public.platform_growth_monthly AS
 SELECT 'geo'::text AS db,
    e.platform,
    e.instrument_model,
    date_trunc('month'::text, (st.published_at)::timestamp with time zone) AS month,
    count(*) AS count
   FROM (public.sra_experiments e
     JOIN public.sra_studies st ON ((st.accession = e.study)))
  WHERE ((st.alias ~~ 'GSE%%'::text) AND (st.published_at IS NOT NULL) AND (e.platform IS NOT NULL) AND (e.platform <> ''::text))
  GROUP BY e.platform, e.instrument_model, (date_trunc('month'::text, (st.published_at)::timestamp with time zone))
UNION ALL
 SELECT 'sra'::text AS db,
    e.platform,
    e.instrument_model,
    date_trunc('month'::text, (st.published_at)::timestamp with time zone) AS month,
    count(*) AS count
   FROM (public.sra_experiments e
     JOIN public.sra_studies st ON ((st.accession = e.study)))
  WHERE (((st.alias IS NULL) OR (st.alias !~~ 'GSE%%'::text)) AND (st.published_at IS NOT NULL) AND (e.platform IS NOT NULL) AND (e.platform <> ''::text))
  GROUP BY e.platform, e.instrument_model, (date_trunc('month'::text, (st.published_at)::timestamp with time zone))
UNION ALL
 SELECT 'ena'::text AS db,
    e.instrument_platform AS platform,
    e.instrument_model,
    date_trunc('month'::text, (e.first_public)::timestamp with time zone) AS month,
    count(*) AS count
   FROM public.ena_experiments e
  WHERE ((e.first_public IS NOT NULL) AND (e.instrument_platform IS NOT NULL) AND (e.instrument_platform <> ''::text))
  GROUP BY e.instrument_platform, e.instrument_model, (date_trunc('month'::text, (e.first_public)::timestamp with time zone))
  WITH NO DATA;


ALTER MATERIALIZED VIEW public.platform_growth_monthly OWNER TO sramap;

--
-- Name: platform_growth_projects_monthly; Type: MATERIALIZED VIEW; Schema: public; Owner: sramap
--

CREATE MATERIALIZED VIEW public.platform_growth_projects_monthly AS
 SELECT 'geo'::text AS db,
    e.platform,
    date_trunc('month'::text, (st.published_at)::timestamp with time zone) AS month,
    count(DISTINCT st.accession) AS count
   FROM (public.sra_experiments e
     JOIN public.sra_studies st ON ((st.accession = e.study)))
  WHERE ((st.alias ~~ 'GSE%%'::text) AND (st.published_at IS NOT NULL) AND (e.platform IS NOT NULL) AND (e.platform <> ''::text))
  GROUP BY e.platform, (date_trunc('month'::text, (st.published_at)::timestamp with time zone))
UNION ALL
 SELECT 'sra'::text AS db,
    e.platform,
    date_trunc('month'::text, (st.published_at)::timestamp with time zone) AS month,
    count(DISTINCT st.accession) AS count
   FROM (public.sra_experiments e
     JOIN public.sra_studies st ON ((st.accession = e.study)))
  WHERE (((st.alias IS NULL) OR (st.alias !~~ 'GSE%%'::text)) AND (st.published_at IS NOT NULL) AND (e.platform IS NOT NULL) AND (e.platform <> ''::text))
  GROUP BY e.platform, (date_trunc('month'::text, (st.published_at)::timestamp with time zone))
UNION ALL
 SELECT 'ena'::text AS db,
    e.instrument_platform AS platform,
    date_trunc('month'::text, (e.first_public)::timestamp with time zone) AS month,
    count(DISTINCT e.study_accession) AS count
   FROM public.ena_experiments e
  WHERE ((e.first_public IS NOT NULL) AND (e.instrument_platform IS NOT NULL) AND (e.instrument_platform <> ''::text))
  GROUP BY e.instrument_platform, (date_trunc('month'::text, (e.first_public)::timestamp with time zone))
  WITH NO DATA;


ALTER MATERIALIZED VIEW public.platform_growth_projects_monthly OWNER TO sramap;

--
-- Name: provenance_samples_v3; Type: TABLE; Schema: public; Owner: sramap
--

CREATE TABLE public.provenance_samples_v3 (
    study_accession text NOT NULL,
    sample text NOT NULL,
    age_source text,
    age_score text,
    age_column text,
    age_original_source text,
    age_original_column text,
    age_original_score text,
    sex_source text,
    sex_score text,
    sex_column text,
    sex_original_source text,
    sex_original_column text,
    sex_original_score text,
    ethnicity_source text,
    ethnicity_score text,
    ethnicity_column text,
    ethnicity_original_source text,
    ethnicity_original_column text,
    ethnicity_original_score text,
    phenotype_source text,
    phenotype_score text,
    phenotype_column text,
    phenotype_original_source text,
    phenotype_original_column text,
    phenotype_original_score text,
    cell_type_source text,
    cell_type_score text,
    cell_type_column text,
    cell_type_original_source text,
    cell_type_original_column text,
    cell_type_original_score text,
    tissue_source text,
    tissue_score text,
    tissue_column text,
    tissue_original_source text,
    tissue_original_column text,
    tissue_original_score text,
    strain_source text,
    strain_score text,
    strain_column text,
    strain_original_source text,
    strain_original_column text,
    strain_original_score text,
    disease_source text,
    disease_score text,
    disease_column text,
    disease_original_source text,
    disease_original_column text,
    disease_original_score text,
    assay_source text,
    assay_score text,
    assay_column text,
    assay_original_source text,
    assay_original_column text,
    assay_original_score text,
    assay_category_source text,
    assay_category_score text,
    assay_category_column text,
    assay_category_original_source text,
    assay_category_original_column text,
    assay_category_original_score text,
    cell_line_source text,
    cell_line_score text,
    cell_line_column text,
    cell_line_original_source text,
    cell_line_original_column text,
    cell_line_original_score text,
    treatment_source text,
    treatment_score text,
    treatment_column text,
    treatment_original_source text,
    treatment_original_column text,
    treatment_original_score text,
    development_stage_source text,
    development_stage_score text,
    development_stage_column text,
    development_stage_original_source text,
    development_stage_original_column text,
    development_stage_original_score text,
    sample_type_source text,
    sample_type_score text,
    sample_type_column text,
    sample_type_original_source text,
    sample_type_original_column text,
    sample_type_original_score text,
    genetic_modification_source text,
    genetic_modification_score text,
    genetic_modification_column text,
    genetic_modification_original_source text,
    genetic_modification_original_column text,
    genetic_modification_original_score text
);


ALTER TABLE public.provenance_samples_v3 OWNER TO sramap;

--
-- Name: pubmed_metadata; Type: TABLE; Schema: public; Owner: sramap
--

CREATE TABLE public.pubmed_metadata (
    pmid text NOT NULL,
    title text,
    journal text,
    doi text,
    pub_date text,
    authors text,
    issn text,
    citation_count integer,
    journal_h_index integer,
    journal_i10_index integer,
    journal_2yr_mean_citedness real,
    journal_cited_by_count integer,
    journal_works_count integer
);


ALTER TABLE public.pubmed_metadata OWNER TO sramap;

--
-- Name: request_log; Type: TABLE; Schema: public; Owner: sramap
--

CREATE TABLE public.request_log (
    id bigint NOT NULL,
    ts timestamp with time zone DEFAULT now() NOT NULL,
    method text,
    path text,
    query_string text,
    search_query text,
    status_code smallint,
    response_ms real,
    ip_address inet,
    ip_hash text,
    country text,
    city text,
    user_agent_raw text,
    browser text,
    browser_version text,
    os text,
    os_version text,
    device_type text,
    accept_language text,
    referer text
);


ALTER TABLE public.request_log OWNER TO sramap;

--
-- Name: request_log_id_seq; Type: SEQUENCE; Schema: public; Owner: sramap
--

CREATE SEQUENCE public.request_log_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.request_log_id_seq OWNER TO sramap;

--
-- Name: request_log_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: sramap
--

ALTER SEQUENCE public.request_log_id_seq OWNED BY public.request_log.id;


--
-- Name: search_vocabulary; Type: TABLE; Schema: public; Owner: sramap
--

CREATE TABLE public.search_vocabulary (
    term text NOT NULL,
    term_type text NOT NULL,
    frequency integer DEFAULT 1,
    normalized text NOT NULL
);


ALTER TABLE public.search_vocabulary OWNER TO sramap;

--
-- Name: sitemap_page_bounds; Type: TABLE; Schema: public; Owner: sramap
--

CREATE TABLE public.sitemap_page_bounds (
    source text NOT NULL,
    page integer NOT NULL,
    start_accession text NOT NULL
);


ALTER TABLE public.sitemap_page_bounds OWNER TO sramap;

--
-- Name: sra_geocode; Type: TABLE; Schema: public; Owner: sramap
--

CREATE TABLE public.sra_geocode (
    center_name text NOT NULL,
    original_country text,
    formatted_address text,
    official_address text,
    place_name text,
    place_type text,
    short_label text,
    loc_name text,
    address_type text,
    osm_id text,
    osm_type text,
    place_id text,
    match_id text,
    longitude double precision,
    latitude double precision,
    city text,
    district text,
    subdistrict text,
    county text,
    suburb text,
    neighbourhood text,
    state text,
    state_district text,
    territory text,
    metro_area text,
    postcode text,
    country_code text,
    country text,
    country_mismatch boolean,
    source text,
    error text,
    multiple_institutes boolean,
    search_vector tsvector GENERATED ALWAYS AS (to_tsvector('english'::regconfig, ((((COALESCE(center_name, ''::text) || ' '::text) || COALESCE(place_name, ''::text)) || ' '::text) || COALESCE(official_address, ''::text)))) STORED
);


ALTER TABLE public.sra_geocode OWNER TO sramap;

--
-- Name: sra_run_download_links; Type: TABLE; Schema: public; Owner: sramap
--

CREATE TABLE public.sra_run_download_links (
    run_accession text NOT NULL,
    study_accession text NOT NULL,
    experiment_accession text,
    library_layout text,
    fastq_ftp text,
    fastq_bytes text,
    fastq_md5 text,
    sra_ftp text,
    sra_bytes text,
    sra_md5 text,
    updated_at text,
    ncbi_files jsonb,
    ncbi_sra_url text,
    ncbi_sra_url_aws text,
    ncbi_fetched_at text,
    ncbi_sra_normalized_url text,
    ncbi_sra_normalized_bytes text,
    ncbi_sra_lite_url text,
    ncbi_sra_lite_bytes text,
    ncbi_sra_lite_s3_url text,
    ncbi_sra_lite_gs_url text
);


ALTER TABLE public.sra_run_download_links OWNER TO sramap;

--
-- Name: sra_runs; Type: TABLE; Schema: public; Owner: sramap
--

CREATE TABLE public.sra_runs (
    accession text NOT NULL,
    experiment text,
    submission text
);


ALTER TABLE public.sra_runs OWNER TO sramap;

--
-- Name: sra_samples; Type: TABLE; Schema: public; Owner: sramap
--

CREATE TABLE public.sra_samples (
    accession text NOT NULL,
    alias text,
    description text,
    title text,
    scientific_name text,
    taxon_id text,
    external_id jsonb,
    attributes_json jsonb,
    submission text,
    links jsonb
);


ALTER TABLE public.sra_samples OWNER TO sramap;

--
-- Name: sra_submissions; Type: TABLE; Schema: public; Owner: sramap
--

CREATE TABLE public.sra_submissions (
    accession text NOT NULL,
    alias text,
    broker_name text,
    center_name text,
    lab_name text,
    published_at date,
    updated_at date
);


ALTER TABLE public.sra_submissions OWNER TO sramap;

--
-- Name: study_publications; Type: TABLE; Schema: public; Owner: sramap
--

CREATE TABLE public.study_publications (
    accession text NOT NULL,
    source text NOT NULL,
    publications jsonb NOT NULL
);


ALTER TABLE public.study_publications OWNER TO sramap;

--
-- Name: term_expansions; Type: TABLE; Schema: public; Owner: sramap
--

CREATE TABLE public.term_expansions (
    id integer NOT NULL,
    short text,
    long text
);


ALTER TABLE public.term_expansions OWNER TO sramap;

--
-- Name: term_expansions_id_seq; Type: SEQUENCE; Schema: public; Owner: sramap
--

CREATE SEQUENCE public.term_expansions_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.term_expansions_id_seq OWNER TO sramap;

--
-- Name: term_expansions_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: sramap
--

ALTER SEQUENCE public.term_expansions_id_seq OWNED BY public.term_expansions.id;


--
-- Name: unified_metadata_centers; Type: TABLE; Schema: public; Owner: sramap
--

CREATE TABLE public.unified_metadata_centers (
    canonical_accession text NOT NULL,
    center_name text,
    "position" integer NOT NULL,
    place_name text,
    place_type text,
    short_label text,
    address_type text,
    longitude double precision,
    latitude double precision,
    city text,
    district text,
    subdistrict text,
    county text,
    suburb text,
    neighbourhood text,
    state text,
    state_district text,
    territory text,
    metro_area text,
    postcode text,
    country_code text,
    country text,
    country_mismatch text,
    geocode_score text
);


ALTER TABLE public.unified_metadata_centers OWNER TO sramap;

--
-- Name: arrayexpress_samples id; Type: DEFAULT; Schema: public; Owner: sramap
--

ALTER TABLE ONLY public.arrayexpress_samples ALTER COLUMN id SET DEFAULT nextval('public.arrayexpress_samples_id_seq'::regclass);


--
-- Name: geo_contributors id; Type: DEFAULT; Schema: public; Owner: sramap
--

ALTER TABLE ONLY public.geo_contributors ALTER COLUMN id SET DEFAULT nextval('public.geo_contributors_id_seq'::regclass);


--
-- Name: request_log id; Type: DEFAULT; Schema: public; Owner: sramap
--

ALTER TABLE ONLY public.request_log ALTER COLUMN id SET DEFAULT nextval('public.request_log_id_seq'::regclass);


--
-- Name: term_expansions id; Type: DEFAULT; Schema: public; Owner: sramap
--

ALTER TABLE ONLY public.term_expansions ALTER COLUMN id SET DEFAULT nextval('public.term_expansions_id_seq'::regclass);


--
-- Name: arrayexpress_experiments arrayexpress_experiments_pkey; Type: CONSTRAINT; Schema: public; Owner: sramap
--

ALTER TABLE ONLY public.arrayexpress_experiments
    ADD CONSTRAINT arrayexpress_experiments_pkey PRIMARY KEY (accession);


--
-- Name: arrayexpress_geocode arrayexpress_geocode_pkey; Type: CONSTRAINT; Schema: public; Owner: sramap
--

ALTER TABLE ONLY public.arrayexpress_geocode
    ADD CONSTRAINT arrayexpress_geocode_pkey PRIMARY KEY (address_key);


--
-- Name: arrayexpress_samples arrayexpress_samples_pkey; Type: CONSTRAINT; Schema: public; Owner: sramap
--

ALTER TABLE ONLY public.arrayexpress_samples
    ADD CONSTRAINT arrayexpress_samples_pkey PRIMARY KEY (id);


--
-- Name: cellosaurus_samples_v3 cellosaurus_samples_v3_pkey; Type: CONSTRAINT; Schema: public; Owner: sramap
--

ALTER TABLE ONLY public.cellosaurus_samples_v3
    ADD CONSTRAINT cellosaurus_samples_v3_pkey PRIMARY KEY (study_accession, sample);


--
-- Name: cluster_labels cluster_labels_pkey; Type: CONSTRAINT; Schema: public; Owner: sramap
--

ALTER TABLE ONLY public.cluster_labels
    ADD CONSTRAINT cluster_labels_pkey PRIMARY KEY (level, cluster_id);


--
-- Name: ena_experiments ena_experiments_pkey; Type: CONSTRAINT; Schema: public; Owner: sramap
--

ALTER TABLE ONLY public.ena_experiments
    ADD CONSTRAINT ena_experiments_pkey PRIMARY KEY (run_accession);


--
-- Name: ena_geocode ena_geocode_pkey; Type: CONSTRAINT; Schema: public; Owner: sramap
--

ALTER TABLE ONLY public.ena_geocode
    ADD CONSTRAINT ena_geocode_pkey PRIMARY KEY (center_name);


--
-- Name: ena_samples ena_samples_pkey; Type: CONSTRAINT; Schema: public; Owner: sramap
--

ALTER TABLE ONLY public.ena_samples
    ADD CONSTRAINT ena_samples_pkey PRIMARY KEY (sample_accession);


--
-- Name: ena_studies ena_studies_pkey; Type: CONSTRAINT; Schema: public; Owner: sramap
--

ALTER TABLE ONLY public.ena_studies
    ADD CONSTRAINT ena_studies_pkey PRIMARY KEY (study_accession);


--
-- Name: enriched_samples enriched_samples_pkey; Type: CONSTRAINT; Schema: public; Owner: sramap
--

ALTER TABLE ONLY public.enriched_samples
    ADD CONSTRAINT enriched_samples_pkey PRIMARY KEY (study_accession, sample);


--
-- Name: enriched_samples_v3 enriched_samples_v3_pkey; Type: CONSTRAINT; Schema: public; Owner: sramap
--

ALTER TABLE ONLY public.enriched_samples_v3
    ADD CONSTRAINT enriched_samples_v3_pkey PRIMARY KEY (study_accession, sample);


--
-- Name: enriched_studies enriched_studies_pkey; Type: CONSTRAINT; Schema: public; Owner: sramap
--

ALTER TABLE ONLY public.enriched_studies
    ADD CONSTRAINT enriched_studies_pkey PRIMARY KEY (accession);


--
-- Name: enriched_studies_v3 enriched_studies_v3_pkey; Type: CONSTRAINT; Schema: public; Owner: sramap
--

ALTER TABLE ONLY public.enriched_studies_v3
    ADD CONSTRAINT enriched_studies_v3_pkey PRIMARY KEY (accession);


--
-- Name: geo_contributors geo_contributors_pkey; Type: CONSTRAINT; Schema: public; Owner: sramap
--

ALTER TABLE ONLY public.geo_contributors
    ADD CONSTRAINT geo_contributors_pkey PRIMARY KEY (id);


--
-- Name: geo_geocode geo_geocode_pkey; Type: CONSTRAINT; Schema: public; Owner: sramap
--

ALTER TABLE ONLY public.geo_geocode
    ADD CONSTRAINT geo_geocode_pkey PRIMARY KEY (address_key);


--
-- Name: geo_platforms geo_platforms_pkey; Type: CONSTRAINT; Schema: public; Owner: sramap
--

ALTER TABLE ONLY public.geo_platforms
    ADD CONSTRAINT geo_platforms_pkey PRIMARY KEY (accession);


--
-- Name: geo_samples geo_samples_pkey; Type: CONSTRAINT; Schema: public; Owner: sramap
--

ALTER TABLE ONLY public.geo_samples
    ADD CONSTRAINT geo_samples_pkey PRIMARY KEY (accession);


--
-- Name: geo_series geo_series_pkey; Type: CONSTRAINT; Schema: public; Owner: sramap
--

ALTER TABLE ONLY public.geo_series
    ADD CONSTRAINT geo_series_pkey PRIMARY KEY (accession);


--
-- Name: low_confidence low_confidence_pkey; Type: CONSTRAINT; Schema: public; Owner: sramap
--

ALTER TABLE ONLY public.low_confidence
    ADD CONSTRAINT low_confidence_pkey PRIMARY KEY (study_accession, sample_accession);


--
-- Name: ontology_samples_v3 ontology_samples_v3_pkey; Type: CONSTRAINT; Schema: public; Owner: sramap
--

ALTER TABLE ONLY public.ontology_samples_v3
    ADD CONSTRAINT ontology_samples_v3_pkey PRIMARY KEY (study_accession, sample);


--
-- Name: provenance_samples_v3 provenance_samples_v3_pkey; Type: CONSTRAINT; Schema: public; Owner: sramap
--

ALTER TABLE ONLY public.provenance_samples_v3
    ADD CONSTRAINT provenance_samples_v3_pkey PRIMARY KEY (study_accession, sample);


--
-- Name: pubmed_metadata pubmed_metadata_pkey; Type: CONSTRAINT; Schema: public; Owner: sramap
--

ALTER TABLE ONLY public.pubmed_metadata
    ADD CONSTRAINT pubmed_metadata_pkey PRIMARY KEY (pmid);


--
-- Name: request_log request_log_pkey; Type: CONSTRAINT; Schema: public; Owner: sramap
--

ALTER TABLE ONLY public.request_log
    ADD CONSTRAINT request_log_pkey PRIMARY KEY (id);


--
-- Name: search_vocabulary search_vocabulary_pkey; Type: CONSTRAINT; Schema: public; Owner: sramap
--

ALTER TABLE ONLY public.search_vocabulary
    ADD CONSTRAINT search_vocabulary_pkey PRIMARY KEY (term);


--
-- Name: sitemap_page_bounds sitemap_page_bounds_pkey; Type: CONSTRAINT; Schema: public; Owner: sramap
--

ALTER TABLE ONLY public.sitemap_page_bounds
    ADD CONSTRAINT sitemap_page_bounds_pkey PRIMARY KEY (source, page);


--
-- Name: sra_experiments sra_experiments_pkey; Type: CONSTRAINT; Schema: public; Owner: sramap
--

ALTER TABLE ONLY public.sra_experiments
    ADD CONSTRAINT sra_experiments_pkey PRIMARY KEY (accession);


--
-- Name: sra_geocode sra_geocode_pkey; Type: CONSTRAINT; Schema: public; Owner: sramap
--

ALTER TABLE ONLY public.sra_geocode
    ADD CONSTRAINT sra_geocode_pkey PRIMARY KEY (center_name);


--
-- Name: sra_run_download_links sra_run_download_links_pkey; Type: CONSTRAINT; Schema: public; Owner: sramap
--

ALTER TABLE ONLY public.sra_run_download_links
    ADD CONSTRAINT sra_run_download_links_pkey PRIMARY KEY (run_accession);


--
-- Name: sra_runs sra_runs_pkey; Type: CONSTRAINT; Schema: public; Owner: sramap
--

ALTER TABLE ONLY public.sra_runs
    ADD CONSTRAINT sra_runs_pkey PRIMARY KEY (accession);


--
-- Name: sra_samples sra_samples_pkey; Type: CONSTRAINT; Schema: public; Owner: sramap
--

ALTER TABLE ONLY public.sra_samples
    ADD CONSTRAINT sra_samples_pkey PRIMARY KEY (accession);


--
-- Name: sra_studies sra_studies_pkey; Type: CONSTRAINT; Schema: public; Owner: sramap
--

ALTER TABLE ONLY public.sra_studies
    ADD CONSTRAINT sra_studies_pkey PRIMARY KEY (accession);


--
-- Name: sra_submissions sra_submissions_pkey; Type: CONSTRAINT; Schema: public; Owner: sramap
--

ALTER TABLE ONLY public.sra_submissions
    ADD CONSTRAINT sra_submissions_pkey PRIMARY KEY (accession);


--
-- Name: study_publications study_publications_pkey; Type: CONSTRAINT; Schema: public; Owner: sramap
--

ALTER TABLE ONLY public.study_publications
    ADD CONSTRAINT study_publications_pkey PRIMARY KEY (accession);


--
-- Name: term_expansions term_expansions_pkey; Type: CONSTRAINT; Schema: public; Owner: sramap
--

ALTER TABLE ONLY public.term_expansions
    ADD CONSTRAINT term_expansions_pkey PRIMARY KEY (id);


--
-- Name: unified_metadata_centers unified_metadata_centers_pkey; Type: CONSTRAINT; Schema: public; Owner: sramap
--

ALTER TABLE ONLY public.unified_metadata_centers
    ADD CONSTRAINT unified_metadata_centers_pkey PRIMARY KEY (canonical_accession, "position");


--
-- Name: unified_metadata unified_metadata_pkey; Type: CONSTRAINT; Schema: public; Owner: sramap
--

ALTER TABLE ONLY public.unified_metadata
    ADD CONSTRAINT unified_metadata_pkey PRIMARY KEY (canonical_accession);


--
-- Name: ae_experiments_accession_upper_idx; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX ae_experiments_accession_upper_idx ON public.arrayexpress_experiments USING btree (upper(accession));


--
-- Name: ae_experiments_external_links_idx; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX ae_experiments_external_links_idx ON public.arrayexpress_experiments USING gin (external_links);


--
-- Name: ae_experiments_fts_idx; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX ae_experiments_fts_idx ON public.arrayexpress_experiments USING gin (tsv);


--
-- Name: ae_experiments_organisms_idx; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX ae_experiments_organisms_idx ON public.arrayexpress_experiments USING gin (organisms);


--
-- Name: ae_experiments_pubmed_idx; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX ae_experiments_pubmed_idx ON public.arrayexpress_experiments USING btree (pubmed_id);


--
-- Name: ae_samples_ena_experiment_idx; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX ae_samples_ena_experiment_idx ON public.arrayexpress_samples USING btree (upper(ena_experiment));


--
-- Name: ae_samples_ena_run_idx; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX ae_samples_ena_run_idx ON public.arrayexpress_samples USING btree (upper(ena_run));


--
-- Name: ae_samples_experiment_idx; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX ae_samples_experiment_idx ON public.arrayexpress_samples USING btree (experiment_accession);


--
-- Name: arrayexpress_experiments_release_date_idx; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX arrayexpress_experiments_release_date_idx ON public.arrayexpress_experiments USING btree (release_date);


--
-- Name: ena_samples_accession_upper_idx; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX ena_samples_accession_upper_idx ON public.ena_samples USING btree (upper(sample_accession));


--
-- Name: ena_studies_first_public_idx; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX ena_studies_first_public_idx ON public.ena_studies USING btree (first_public);


--
-- Name: ena_studies_search_fts_idx; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX ena_studies_search_fts_idx ON public.ena_studies_search USING gin (tsv);


--
-- Name: geo_contributors_accession_idx; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX geo_contributors_accession_idx ON public.geo_contributors USING btree (accession);


--
-- Name: geo_contributors_org_trgm_idx; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX geo_contributors_org_trgm_idx ON public.geo_contributors USING gin (organization public.gin_trgm_ops);


--
-- Name: geo_samples_accession_idx; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX geo_samples_accession_idx ON public.geo_samples USING btree (accession);


--
-- Name: geo_series_accession_idx; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX geo_series_accession_idx ON public.geo_series USING btree (accession);


--
-- Name: geo_series_accession_upper_idx; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX geo_series_accession_upper_idx ON public.geo_series USING btree (upper(accession));


--
-- Name: geo_series_bioproject_upper_idx; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX geo_series_bioproject_upper_idx ON public.geo_series USING btree (upper(bioproject_id));


--
-- Name: geo_series_fts_idx; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX geo_series_fts_idx ON public.geo_series USING gin (tsv);


--
-- Name: geo_series_published_at_idx; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX geo_series_published_at_idx ON public.geo_series USING btree (published_at);


--
-- Name: geo_series_samples_ref_idx; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX geo_series_samples_ref_idx ON public.geo_series USING gin (samples_ref);


--
-- Name: geo_series_sim_idx; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX geo_series_sim_idx ON public.geo_series USING gin ((((((title || ' '::text) || summary) || ' '::text) || overall_design)) public.gin_trgm_ops);


--
-- Name: idx_ae_experiments_release_date; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_ae_experiments_release_date ON public.arrayexpress_experiments USING btree (release_date);


--
-- Name: idx_ae_geocode_country; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_ae_geocode_country ON public.arrayexpress_geocode USING btree (geo_country);


--
-- Name: idx_ae_geocode_org; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_ae_geocode_org ON public.arrayexpress_geocode USING btree (organization);


--
-- Name: idx_ae_geocode_search; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_ae_geocode_search ON public.arrayexpress_geocode USING gin (search_vector);


--
-- Name: idx_cellosaurus_v3_disease; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_cellosaurus_v3_disease ON public.cellosaurus_samples_v3 USING btree (disease);


--
-- Name: idx_cellosaurus_v3_organism; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_cellosaurus_v3_organism ON public.cellosaurus_samples_v3 USING btree (organism);


--
-- Name: idx_cellosaurus_v3_study; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_cellosaurus_v3_study ON public.cellosaurus_samples_v3 USING btree (study_accession);


--
-- Name: idx_cellosaurus_v3_tissue; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_cellosaurus_v3_tissue ON public.cellosaurus_samples_v3 USING btree (tissue);


--
-- Name: idx_common_names_common_name; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_common_names_common_name ON public.common_names USING btree (scientific_name);


--
-- Name: idx_common_names_scientific_name; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_common_names_scientific_name ON public.common_names USING btree (scientific_name);


--
-- Name: idx_common_names_tax_id; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_common_names_tax_id ON public.common_names USING btree (tax_id);


--
-- Name: idx_cross_ref_ae_from; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_cross_ref_ae_from ON public.cross_ref_ae USING btree (ae_accession);


--
-- Name: idx_cross_ref_ae_to; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_cross_ref_ae_to ON public.cross_ref_ae USING btree (target_accession);


--
-- Name: idx_cross_ref_ena_from; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_cross_ref_ena_from ON public.cross_ref_ena USING btree (ena_accession);


--
-- Name: idx_cross_ref_ena_to; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_cross_ref_ena_to ON public.cross_ref_ena USING btree (target_accession);


--
-- Name: idx_cross_ref_geo_from; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_cross_ref_geo_from ON public.cross_ref_geo USING btree (geo_accession);


--
-- Name: idx_cross_ref_geo_to; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_cross_ref_geo_to ON public.cross_ref_geo USING btree (target_accession);


--
-- Name: idx_ena_exp_country; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_ena_exp_country ON public.ena_experiments USING btree (country);


--
-- Name: idx_ena_exp_experiment; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_ena_exp_experiment ON public.ena_experiments USING btree (experiment_accession);


--
-- Name: idx_ena_exp_platform; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_ena_exp_platform ON public.ena_experiments USING btree (instrument_platform);


--
-- Name: idx_ena_exp_sample; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_ena_exp_sample ON public.ena_experiments USING btree (sample_accession);


--
-- Name: idx_ena_exp_strategy; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_ena_exp_strategy ON public.ena_experiments USING btree (library_strategy);


--
-- Name: idx_ena_exp_study; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_ena_exp_study ON public.ena_experiments USING btree (study_accession);


--
-- Name: idx_ena_experiments_first_public; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_ena_experiments_first_public ON public.ena_experiments USING btree (first_public);


--
-- Name: idx_ena_studies_center_name; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_ena_studies_center_name ON public.ena_studies USING btree (center_name);


--
-- Name: idx_ena_studies_first_public; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_ena_studies_first_public ON public.ena_studies USING btree (first_public);


--
-- Name: idx_ena_studies_geo_accession; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_ena_studies_geo_accession ON public.ena_studies USING btree (geo_accession);


--
-- Name: idx_ena_studies_scientific_name; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_ena_studies_scientific_name ON public.ena_studies USING btree (scientific_name);


--
-- Name: idx_enriched_crosstab_pair; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_enriched_crosstab_pair ON public.enriched_crosstab USING btree (group_col, breakdown_col, group_total DESC);


--
-- Name: idx_enriched_studies_modality; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_enriched_studies_modality ON public.enriched_studies USING hash (single_cell_modality);


--
-- Name: idx_enriched_top_values_col_n; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_enriched_top_values_col_n ON public.enriched_column_top_values USING btree (column_name, n DESC);


--
-- Name: idx_enriched_v3_assay; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_enriched_v3_assay ON public.enriched_samples_v3 USING btree (assay);


--
-- Name: idx_enriched_v3_cell_line; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_enriched_v3_cell_line ON public.enriched_samples_v3 USING btree (cell_line);


--
-- Name: idx_enriched_v3_cell_type; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_enriched_v3_cell_type ON public.enriched_samples_v3 USING btree (cell_type);


--
-- Name: idx_enriched_v3_disease; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_enriched_v3_disease ON public.enriched_samples_v3 USING btree (disease);


--
-- Name: idx_enriched_v3_organism; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_enriched_v3_organism ON public.enriched_samples_v3 USING btree (genetic_modification);


--
-- Name: idx_enriched_v3_sample_type; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_enriched_v3_sample_type ON public.enriched_samples_v3 USING btree (sample_type);


--
-- Name: idx_enriched_v3_study; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_enriched_v3_study ON public.enriched_samples_v3 USING btree (study_accession);


--
-- Name: idx_enriched_v3_tissue; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_enriched_v3_tissue ON public.enriched_samples_v3 USING hash (tissue);


--
-- Name: idx_experiments_library_strategy; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_experiments_library_strategy ON public.sra_experiments USING btree (library_strategy);


--
-- Name: idx_experiments_submission_strategy; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_experiments_submission_strategy ON public.sra_experiments USING btree (submission, library_strategy);


--
-- Name: idx_geo_contributors_country; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_geo_contributors_country ON public.geo_contributors USING btree (address_country);


--
-- Name: idx_geo_geocode_country; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_geo_geocode_country ON public.geo_geocode USING btree (geo_country);


--
-- Name: idx_geo_geocode_org; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_geo_geocode_org ON public.geo_geocode USING btree (organization);


--
-- Name: idx_geo_geocode_search; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_geo_geocode_search ON public.geo_geocode USING gin (search_vector);


--
-- Name: idx_geo_platforms_technology; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_geo_platforms_technology ON public.geo_platforms USING btree (technology);


--
-- Name: idx_geo_samples_published_at; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_geo_samples_published_at ON public.geo_samples USING btree (published_at);


--
-- Name: idx_geo_series_bioproject_id; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_geo_series_bioproject_id ON public.geo_series USING btree (bioproject_id);


--
-- Name: idx_geo_series_organisms; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_geo_series_organisms ON public.geo_series USING gin (organisms);


--
-- Name: idx_geo_series_platforms; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_geo_series_platforms ON public.geo_series USING gin (platforms);


--
-- Name: idx_geo_series_published_at; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_geo_series_published_at ON public.geo_series USING btree (published_at);


--
-- Name: idx_geo_series_samples_ref_gin; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_geo_series_samples_ref_gin ON public.geo_series USING gin (samples_ref);


--
-- Name: idx_ogm_organism; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_ogm_organism ON public.organism_growth_monthly USING btree (organism);


--
-- Name: idx_ogm_organism_db_month; Type: INDEX; Schema: public; Owner: sramap
--

CREATE UNIQUE INDEX idx_ogm_organism_db_month ON public.organism_growth_monthly USING btree (organism, db, month);


--
-- Name: idx_ontology_v3_assay_id; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_ontology_v3_assay_id ON public.ontology_samples_v3 USING btree (assay_ontology_id);


--
-- Name: idx_ontology_v3_cell_type_id; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_ontology_v3_cell_type_id ON public.ontology_samples_v3 USING btree (cell_type_ontology_id);


--
-- Name: idx_ontology_v3_disease_id; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_ontology_v3_disease_id ON public.ontology_samples_v3 USING btree (disease_ontology_id);


--
-- Name: idx_ontology_v3_organism; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_ontology_v3_organism ON public.ontology_samples_v3 USING btree (organism);


--
-- Name: idx_ontology_v3_study; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_ontology_v3_study ON public.ontology_samples_v3 USING btree (study_accession);


--
-- Name: idx_ontology_v3_tissue_id; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_ontology_v3_tissue_id ON public.ontology_samples_v3 USING btree (tissue_ontology_id);


--
-- Name: idx_pgm_platform; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_pgm_platform ON public.platform_growth_monthly USING btree (platform);


--
-- Name: idx_pgm_platform_db_month_model; Type: INDEX; Schema: public; Owner: sramap
--

CREATE UNIQUE INDEX idx_pgm_platform_db_month_model ON public.platform_growth_monthly USING btree (platform, db, month, instrument_model);


--
-- Name: idx_pgpm_platform_db_month; Type: INDEX; Schema: public; Owner: sramap
--

CREATE UNIQUE INDEX idx_pgpm_platform_db_month ON public.platform_growth_projects_monthly USING btree (platform, db, month);


--
-- Name: idx_provenance_v3_study; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_provenance_v3_study ON public.provenance_samples_v3 USING btree (study_accession);


--
-- Name: idx_request_log_city; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_request_log_city ON public.request_log USING btree (city);


--
-- Name: idx_request_log_country; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_request_log_country ON public.request_log USING btree (country);


--
-- Name: idx_request_log_device_type; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_request_log_device_type ON public.request_log USING btree (device_type);


--
-- Name: idx_request_log_ip_address; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_request_log_ip_address ON public.request_log USING btree (ip_address);


--
-- Name: idx_request_log_path; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_request_log_path ON public.request_log USING btree (path);


--
-- Name: idx_request_log_search_query; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_request_log_search_query ON public.request_log USING btree (search_query);


--
-- Name: idx_request_log_ts; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_request_log_ts ON public.request_log USING btree (ts);


--
-- Name: idx_samples_scientific_name; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_samples_scientific_name ON public.sra_samples USING btree (scientific_name);


--
-- Name: idx_samples_submission; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_samples_submission ON public.sra_samples USING btree (submission);


--
-- Name: idx_sra_dl_ena_ncbi; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_sra_dl_ena_ncbi ON public.sra_run_download_links USING btree (fastq_ftp, ncbi_sra_url);


--
-- Name: idx_sra_dl_experiment; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_sra_dl_experiment ON public.sra_run_download_links USING btree (experiment_accession);


--
-- Name: idx_sra_dl_fastq_ftp; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_sra_dl_fastq_ftp ON public.sra_run_download_links USING btree (fastq_ftp);


--
-- Name: idx_sra_dl_ncbi_sra_url; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_sra_dl_ncbi_sra_url ON public.sra_run_download_links USING btree (ncbi_sra_url);


--
-- Name: idx_sra_dl_study; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_sra_dl_study ON public.sra_run_download_links USING btree (study_accession);


--
-- Name: idx_sra_exp_library_strategy; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_sra_exp_library_strategy ON public.sra_experiments USING btree (library_strategy);


--
-- Name: idx_sra_exp_platform; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_sra_exp_platform ON public.sra_experiments USING btree (platform);


--
-- Name: idx_sra_experiments_samples_gin; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_sra_experiments_samples_gin ON public.sra_experiments USING gin (samples);


--
-- Name: idx_sra_geocode_country; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_sra_geocode_country ON public.sra_geocode USING btree (country);


--
-- Name: idx_sra_geocode_search; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_sra_geocode_search ON public.sra_geocode USING gin (search_vector);


--
-- Name: idx_sra_studies_bioproject_id; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_sra_studies_bioproject_id ON public.sra_studies USING btree (bioproject_id);


--
-- Name: idx_sra_studies_canonical; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_sra_studies_canonical ON public.sra_studies USING btree (COALESCE(alias, accession));


--
-- Name: idx_sra_studies_organisms; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_sra_studies_organisms ON public.sra_studies USING gin (organisms);


--
-- Name: idx_sra_studies_platforms; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_sra_studies_platforms ON public.sra_studies USING gin (platforms);


--
-- Name: idx_sra_studies_published_at; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_sra_studies_published_at ON public.sra_studies USING btree (published_at);


--
-- Name: idx_sra_submissions_center; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_sra_submissions_center ON public.sra_submissions USING btree (center_name);


--
-- Name: idx_study_publications_source; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_study_publications_source ON public.study_publications USING btree (source);


--
-- Name: idx_term_expansions_long; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_term_expansions_long ON public.term_expansions USING btree (long);


--
-- Name: idx_term_expansions_short; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_term_expansions_short ON public.term_expansions USING btree (short);


--
-- Name: idx_um_citation_count; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_um_citation_count ON public.unified_metadata USING btree (citation_count);


--
-- Name: idx_um_country; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_um_country ON public.unified_metadata USING btree (country);


--
-- Name: idx_um_country_code; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_um_country_code ON public.unified_metadata USING btree (country_code);


--
-- Name: idx_um_country_code_iso2; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_um_country_code_iso2 ON public.unified_metadata USING btree (country_code_iso2);


--
-- Name: idx_um_dominant_common; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_um_dominant_common ON public.unified_metadata USING btree (dominant_common_name);


--
-- Name: idx_um_dominant_l1; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_um_dominant_l1 ON public.unified_metadata USING btree (dominant_assay_l1);


--
-- Name: idx_um_dominant_l2; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_um_dominant_l2 ON public.unified_metadata USING btree (dominant_assay_l2);


--
-- Name: idx_um_dominant_sci; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_um_dominant_sci ON public.unified_metadata USING btree (dominant_scientific_name);


--
-- Name: idx_um_first_published; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_um_first_published ON public.unified_metadata USING btree (first_published);


--
-- Name: idx_um_is_single_cell; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_um_is_single_cell ON public.unified_metadata USING btree (is_single_cell);


--
-- Name: idx_um_modality; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_um_modality ON public.unified_metadata USING btree (single_cell_modality);


--
-- Name: idx_um_place_type; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_um_place_type ON public.unified_metadata USING btree (place_type);


--
-- Name: idx_um_pmid; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_um_pmid ON public.unified_metadata USING btree (pmid);


--
-- Name: idx_um_source; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_um_source ON public.unified_metadata USING btree (source);


--
-- Name: idx_umc_accession; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_umc_accession ON public.unified_metadata_centers USING btree (canonical_accession);


--
-- Name: idx_umc_center; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_umc_center ON public.unified_metadata_centers USING btree (center_name);


--
-- Name: idx_umc_country; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_umc_country ON public.unified_metadata_centers USING btree (country);


--
-- Name: idx_umc_country_code; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_umc_country_code ON public.unified_metadata_centers USING btree (country_code);


--
-- Name: idx_vocab_normalized; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_vocab_normalized ON public.search_vocabulary USING btree (normalized);


--
-- Name: idx_vocab_trgm; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX idx_vocab_trgm ON public.search_vocabulary USING gist (normalized public.gist_trgm_ops);


--
-- Name: sra_experiments_accession_upper_idx; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX sra_experiments_accession_upper_idx ON public.sra_experiments USING btree (upper(accession));


--
-- Name: sra_experiments_samples_idx; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX sra_experiments_samples_idx ON public.sra_experiments USING gin (samples);


--
-- Name: sra_experiments_study_idx; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX sra_experiments_study_idx ON public.sra_experiments USING btree (study);


--
-- Name: sra_studies_accession_upper_idx; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX sra_studies_accession_upper_idx ON public.sra_studies USING btree (upper(accession));


--
-- Name: sra_studies_alias_idx; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX sra_studies_alias_idx ON public.sra_studies USING btree (alias);


--
-- Name: sra_studies_alias_upper_idx; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX sra_studies_alias_upper_idx ON public.sra_studies USING btree (upper(alias));


--
-- Name: sra_studies_fts_idx; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX sra_studies_fts_idx ON public.sra_studies USING gin (tsv);


--
-- Name: sra_studies_published_at_idx; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX sra_studies_published_at_idx ON public.sra_studies USING btree (published_at);


--
-- Name: sra_studies_sim_idx; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX sra_studies_sim_idx ON public.sra_studies USING gin ((((((title || ' '::text) || abstract) || ' '::text) || study_type)) public.gin_trgm_ops);


--
-- Name: sra_studies_submission_idx; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX sra_studies_submission_idx ON public.sra_studies USING btree (submission);


--
-- Name: sra_submissions_center_trgm_idx; Type: INDEX; Schema: public; Owner: sramap
--

CREATE INDEX sra_submissions_center_trgm_idx ON public.sra_submissions USING gin (center_name public.gin_trgm_ops);


--
-- Name: geo_series geo_series_tsv_trigger; Type: TRIGGER; Schema: public; Owner: sramap
--

CREATE TRIGGER geo_series_tsv_trigger BEFORE INSERT OR UPDATE ON public.geo_series FOR EACH ROW EXECUTE FUNCTION public.geo_series_tsv_update();


--
-- PostgreSQL database dump complete
--

\unrestrict W8Zzfk6Wj3Vv16DZOge0MC1iIVKAS43MPugSIJUzZs9cTL2d8bTAMJNwM0f3IyB
