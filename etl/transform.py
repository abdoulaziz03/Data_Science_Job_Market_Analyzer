"""
etl/transform.py
Pipeline ETL : Extraction → Transformation → Chargement (RDS + S3)
"""

import os
import re
import json
import boto3
import pandas as pd
import sqlalchemy as sa
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
from dotenv import load_dotenv
load_dotenv(override=True)

# ─── Configuration ─────────────────────────────────────────────────────────────
RAW_PATH = "data/raw/jobs_raw.csv"
CLEAN_PATH = "data/processed/jobs_clean.csv"

# Compétences à détecter dans les descriptions
SKILLS_KEYWORDS = [
    "python", "sql", "r", "spark", "hadoop", "airflow", "kafka",
    "tensorflow", "pytorch", "scikit-learn", "pandas", "numpy",
    "tableau", "power bi", "looker", "dbt", "aws", "gcp", "azure",
    "docker", "kubernetes", "git", "mlflow", "nlp", "llm", "machine learning",
    "deep learning", "data lake", "data warehouse", "etl", "api"
]


# ─── 1. EXTRACTION ─────────────────────────────────────────────────────────────
def extract(path: str = RAW_PATH) -> pd.DataFrame:
    """Charge le CSV brut collecté par le scraper."""
    print(f"📥 Extraction depuis : {path}")
    df = pd.read_csv(path, encoding="utf-8-sig")
    print(f"   → {len(df)} lignes chargées, {df.shape[1]} colonnes")
    return df


# ─── 2. TRANSFORMATION ─────────────────────────────────────────────────────────
def clean_text(text) -> str:
    """Nettoie une chaîne de caractères."""
    if pd.isna(text):
        return ""
    return str(text).strip().title()


def normalize_location(location: str) -> str:
    """Simplifie la localisation (garde ville principale)."""
    if not location:
        return "Non précisé"
    # Garde uniquement la première partie avant la virgule
    return location.split(",")[0].strip().title()


def extract_salary_avg(row) -> float | None:
    """Calcule le salaire moyen si min et max sont disponibles."""
    s_min = pd.to_numeric(row.get("salary_min"), errors="coerce")
    s_max = pd.to_numeric(row.get("salary_max"), errors="coerce")
    if pd.notna(s_min) and pd.notna(s_max):
        return round((s_min + s_max) / 2, 2)
    if pd.notna(s_min):
        return s_min
    if pd.notna(s_max):
        return s_max
    return None


def extract_skills(description: str) -> list[str]:
    """Extrait les compétences techniques mentionnées dans la description."""
    if not description:
        return []
    desc_lower = description.lower()
    return [skill for skill in SKILLS_KEYWORDS if skill in desc_lower]


def normalize_contract(contract: str) -> str:
    """Normalise le type de contrat."""
    if not contract or pd.isna(contract):
        return "Non précisé"
    contract = str(contract).lower()
    if "full" in contract or "permanent" in contract or "cdi" in contract:
        return "CDI"
    if "part" in contract or "cdd" in contract:
        return "CDD"
    if "freelance" in contract or "contract" in contract:
        return "Freelance"
    return contract.title()


def transform(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Applique toutes les transformations et retourne (jobs_df, skills_df)."""
    print("\n🔄 Transformation en cours...")

    # Suppression des doublons et lignes sans titre
    df.drop_duplicates(subset=["title", "company"], inplace=True)
    df.dropna(subset=["title"], inplace=True)

    # Nettoyage des colonnes texte
    df["title"]    = df["title"].apply(clean_text)
    df["company"]  = df["company"].apply(clean_text)
    df["location"] = df["location"].apply(
        lambda x: normalize_location(clean_text(x))
    )
    df["contract"] = df["contract"].apply(normalize_contract)

    # Salaire moyen calculé
    df["salary_avg"] = df.apply(extract_salary_avg, axis=1)

    # Conversion des salaires en numérique
    df["salary_min"] = pd.to_numeric(df["salary_min"], errors="coerce")
    df["salary_max"] = pd.to_numeric(df["salary_max"], errors="coerce")

    # Suppression des salaires aberrants (< 1000 ou > 500 000 €)
    mask = df["salary_avg"].notna()
    df.loc[mask & ((df["salary_avg"] < 1000) | (df["salary_avg"] > 500_000)), 
           ["salary_min", "salary_max", "salary_avg"]] = None

    # Date de parsing
    df["created_at"] = pd.to_datetime(df["created"], errors="coerce").dt.date

    # Description nettoyée
    df["description"] = df["description"].fillna("").str.strip()

    # Extraction des compétences → table séparée
    df["skills_list"] = df["description"].apply(extract_skills)

    # Table job_skills (format long)
    skills_rows = []
    for idx, row in df.iterrows():
        for skill in row["skills_list"]:
            skills_rows.append({"job_index": idx, "skill": skill})
    skills_df = pd.DataFrame(skills_rows)

    # Colonnes finales pour la table jobs
    jobs_df = df[[
        "source", "title", "company", "location",
        "salary_min", "salary_max", "salary_avg",
        "contract", "category", "description", "url", "created_at"
    ]].copy()

    print(f"   → {len(jobs_df)} offres nettoyées")
    print(f"   → {len(skills_df)} entrées de compétences extraites")
    print(f"   → Salaires renseignés : {jobs_df['salary_avg'].notna().sum()}")

    return jobs_df, skills_df


# ─── 3. CHARGEMENT ─────────────────────────────────────────────────────────────
def load_to_csv(df: pd.DataFrame, path: str = CLEAN_PATH):
    """Sauvegarde locale du CSV nettoyé."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"\n💾 CSV propre sauvegardé : {path}")


def load_to_s3(df: pd.DataFrame):
    """Upload le CSV nettoyé vers AWS S3 (Data Lake)."""
    bucket = os.getenv("S3_BUCKET_NAME")
    if not bucket:
        print("⚠️  S3_BUCKET_NAME non défini, upload S3 ignoré.")
        return

    try:
        s3 = boto3.client(
            "s3",
            region_name=os.getenv("AWS_REGION", "eu-west-3"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        )
        key = f"processed/jobs_clean_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig"),
            ContentType="text/csv",
        )
        print(f"☁️  Fichier uploadé sur S3 : s3://{bucket}/{key}")
    except Exception as e:
        print(f"❌ Erreur upload S3 : {e}")


def load_to_rds(jobs_df: pd.DataFrame, skills_df: pd.DataFrame):
    """Charge les données dans PostgreSQL (AWS RDS)."""
    db_url = (
        f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', 5432)}/{os.getenv('DB_NAME')}"
    )
    if not os.getenv("DB_HOST"):
        print("⚠️  DB_HOST non défini, chargement RDS ignoré.")
        return

    try:
        engine = sa.create_engine(db_url)
        jobs_df.to_sql("jobs", engine, if_exists="append", index=False)
        print(f"✅ {len(jobs_df)} offres insérées dans RDS (table: jobs)")

        # Récupère les IDs insérés pour lier les compétences
        if not skills_df.empty:
            with engine.connect() as conn:
                result = conn.execute(
                    sa.text("SELECT id FROM jobs ORDER BY inserted_at DESC LIMIT :n"),
                    {"n": len(jobs_df)}
                )
                job_ids = [r[0] for r in result][::-1]

            id_map = {idx: job_ids[i] for i, idx in enumerate(jobs_df.index)}
            skills_df["job_id"] = skills_df["job_index"].map(id_map)
            skills_df[["job_id", "skill"]].dropna().to_sql(
                "job_skills", engine, if_exists="append", index=False
            )
            print(f"✅ {len(skills_df)} compétences insérées dans RDS (table: job_skills)")

    except Exception as e:
        print(f"❌ Erreur RDS : {e}")


# ─── Pipeline principal ────────────────────────────────────────────────────────
def main():
    print("=" * 55)
    print("  PIPELINE ETL - Offres Data Science")
    print("=" * 55)

    df = extract()
    jobs_df, skills_df = transform(df)

    load_to_csv(jobs_df)
    load_to_s3(jobs_df)
    load_to_rds(jobs_df, skills_df)

    print("\n✅ Pipeline ETL terminé avec succès !")


if __name__ == "__main__":
    main()