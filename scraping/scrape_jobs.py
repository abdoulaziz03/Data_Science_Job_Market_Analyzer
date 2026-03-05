import requests
import pandas as pd
import time
import os
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# ─── Configuration ────────────────────────────────────────────────────────────
load_dotenv()  # Charge les variables depuis .env (jamais de clés en dur !)

APP_ID  = os.getenv("ADZUNA_APP_ID") 
APP_KEY = os.getenv("ADZUNA_APP_KEY")
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

# ─── 1. API Adzuna (multi-pages) ───────────────────────────────────────────────
def fetch_adzuna(query: str = "data scientist", country: str = "fr", max_pages: int = 5) -> list[dict]:
    """Récupère les offres d'emploi via l'API Adzuna sur plusieurs pages."""
    jobs = []

    for page in range(1, max_pages + 1):
        url = (
            f"https://api.adzuna.com/v1/api/jobs/{country}/search/{page}"
            f"?app_id={APP_ID}&app_key={APP_KEY}"
            f"&what={query.replace(' ', '%20')}&results_per_page=50"
        )
        try:
            response = requests.get(url, headers=HEADERS, timeout=10)
            response.raise_for_status()
            data = response.json()

            results = data.get("results", [])
            if not results:
                print(f"  [Adzuna] Page {page} vide, arrêt.")
                break

            for job in results:
                jobs.append({
                    "source":      "Adzuna",
                    "title":       job.get("title", "").strip(),
                    "company":     job.get("company", {}).get("display_name", "").strip(),
                    "location":    job.get("location", {}).get("display_name", "").strip(),
                    "salary_min":  job.get("salary_min"),
                    "salary_max":  job.get("salary_max"),
                    "description": job.get("description", "").strip()[:500],
                    "url":         job.get("redirect_url", ""),
                    "created":     job.get("created", ""),
                    "contract":    job.get("contract_time", ""),
                    "category":    job.get("category", {}).get("label", ""),
                })

            print(f"  [Adzuna] Page {page} → {len(results)} offres")
            time.sleep(1)  # Respect du rate limit

        except requests.exceptions.RequestException as e:
            print(f"  [Adzuna] Erreur page {page} : {e}")
            break

    return jobs


# ─── 2. Scraping AI-Jobs.net (page statique) ──────────────────────────────────
def scrape_aijobs(max_pages: int = 3) -> list[dict]:
    jobs = []

    for page in range(1, max_pages + 1):
        # ← URL corrigée
        url = f"https://aijobs.net/list/data-scientist-jobs/?page={page}"
        try:
            response = requests.get(url, headers=HEADERS, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")

            cards = soup.select("li.job")
            if not cards:
                print(f"  [AI-Jobs] Page {page} vide ou structure modifiée.")
                break

            for card in cards:
                title_tag    = card.select_one("h2.job-title a")
                company_tag  = card.select_one("span.company-name")
                location_tag = card.select_one("span.job-location")
                salary_tag   = card.select_one("span.salary")

                jobs.append({
                    "source":      "AI-Jobs.net",
                    "title":       title_tag.get_text(strip=True)    if title_tag    else "",
                    "company":     company_tag.get_text(strip=True)  if company_tag  else "",
                    "location":    location_tag.get_text(strip=True) if location_tag else "",
                    "salary_min":  None,
                    "salary_max":  None,
                    "description": salary_tag.get_text(strip=True)   if salary_tag   else "",
                    "url":         "https://aijobs.net" + title_tag["href"] if title_tag else "",
                    "created":     "",
                    "contract":    "",
                    "category":    "Data Science",
                })

            print(f"  [AI-Jobs] Page {page} → {len(cards)} offres")
            time.sleep(1.5)

        except requests.exceptions.RequestException as e:
            print(f"  [AI-Jobs] Erreur page {page} : {e}")
            break

    return jobs


# ─── 3. Pipeline principal ────────────────────────────────────────────────────
def main():
    print("=== Démarrage de la collecte des offres d'emploi ===\n")

    all_jobs = []

    # API Adzuna
    print("📡 Collecte via API Adzuna...")
    adzuna_jobs = fetch_adzuna(query="data scientist", max_pages=5)
    all_jobs.extend(adzuna_jobs)
    print(f"  → Total Adzuna : {len(adzuna_jobs)} offres\n")

    # Scraping AI-Jobs
    print("🕷️  Scraping AI-Jobs.net...")
    aijobs = scrape_aijobs(max_pages=3)
    all_jobs.extend(aijobs)
    print(f"  → Total AI-Jobs : {len(aijobs)} offres\n")

    # Création du DataFrame
    df = pd.DataFrame(all_jobs)

    # Dédoublonnage sur titre + entreprise
    before = len(df)
    df.drop_duplicates(subset=["title", "company"], inplace=True)
    print(f"✅ Dédoublonnage : {before} → {len(df)} offres\n")

    # Sauvegarde
    output_path = "../data/raw/jobs_raw.csv"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8-sig")

    print(f"💾 Fichier sauvegardé : {output_path}")
    print(f"📊 Total final : {len(df)} offres\n")
    print(df[["source", "title", "company", "location"]].head(10).to_string(index=False))


if __name__ == "__main__":
    main()
