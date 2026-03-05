-- ============================================================
--  Jobs Data Warehouse - Schéma relationnel
--  Compatible PostgreSQL (AWS RDS) et SQLite (local dev)
-- ============================================================

-- Table principale des offres d'emploi
CREATE TABLE IF NOT EXISTS jobs (
    id            SERIAL PRIMARY KEY,
    source        VARCHAR(50)   NOT NULL,          -- Adzuna, AI-Jobs.net...
    title         VARCHAR(255)  NOT NULL,
    company       VARCHAR(255),
    location      VARCHAR(255),
    salary_min    NUMERIC(10,2),
    salary_max    NUMERIC(10,2),
    salary_avg    NUMERIC(10,2),                   -- calculé à l'ETL
    contract      VARCHAR(100),                    -- CDI, CDD, freelance...
    category      VARCHAR(100),
    description   TEXT,
    url           TEXT,
    created_at    DATE,
    inserted_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table des compétences extraites des descriptions
CREATE TABLE IF NOT EXISTS job_skills (
    id       SERIAL PRIMARY KEY,
    job_id   INT REFERENCES jobs(id) ON DELETE CASCADE,
    skill    VARCHAR(100) NOT NULL
);

-- Table des localisations normalisées (pour les analyses géo)
CREATE TABLE IF NOT EXISTS locations (
    id          SERIAL PRIMARY KEY,
    raw_name    VARCHAR(255) UNIQUE NOT NULL,
    city        VARCHAR(100),
    region      VARCHAR(100),
    country     VARCHAR(100) DEFAULT 'France'
);

-- Vue analytique : offres avec salaire moyen et compétences agrégées
CREATE OR REPLACE VIEW v_jobs_summary AS
SELECT
    j.id,
    j.source,
    j.title,
    j.company,
    j.location,
    j.salary_avg,
    j.contract,
    j.category,
    j.created_at,
    STRING_AGG(s.skill, ', ') AS skills
FROM jobs j
LEFT JOIN job_skills s ON s.job_id = j.id
GROUP BY j.id, j.source, j.title, j.company,
         j.location, j.salary_avg, j.contract,
         j.category, j.created_at;

-- Index pour accélérer les requêtes fréquentes
CREATE INDEX IF NOT EXISTS idx_jobs_location  ON jobs(location);
CREATE INDEX IF NOT EXISTS idx_jobs_source    ON jobs(source);
CREATE INDEX IF NOT EXISTS idx_jobs_created   ON jobs(created_at);
CREATE INDEX IF NOT EXISTS idx_skills_job_id  ON job_skills(job_id);