"""
infrastructure/aws_setup.py
Crée et configure automatiquement l'infrastructure AWS :
  - S3 Bucket (Data Lake)
  - RDS PostgreSQL (Data Warehouse)
  - IAM + sécurité de base

Pré-requis : variables dans .env configurées
Lancement   : python infrastructure/aws_setup.py
"""

import os
import json
import time
import boto3
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# ─── Configuration ─────────────────────────────────────────────────────────────
REGION      = os.getenv("AWS_REGION", "eu-west-3")
BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "jobs-data-lake")
DB_NAME     = os.getenv("DB_NAME",     "jobs_db")
DB_USER = os.getenv("DB_USER", "dbadmin")
DB_PASSWORD = os.getenv("DB_PASSWORD", "changeme123!")
DB_PORT     = int(os.getenv("DB_PORT", 5432))

# Clients AWS
session = boto3.Session(
    aws_access_key_id     = os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name           = REGION,
)
s3  = session.client("s3")
rds = session.client("rds")
ec2 = session.client("ec2")


# ══════════════════════════════════════════════════════════════════════════════
#  1. S3 — Data Lake
# ══════════════════════════════════════════════════════════════════════════════
def create_s3_bucket():
    print("\n📦 Création du bucket S3 (Data Lake)...")
    try:
        if REGION == "us-east-1":
            s3.create_bucket(Bucket=BUCKET_NAME)
        else:
            s3.create_bucket(
                Bucket=BUCKET_NAME,
                CreateBucketConfiguration={"LocationConstraint": REGION},
            )

        # Bloquer tout accès public
        s3.put_public_access_block(
            Bucket=BUCKET_NAME,
            PublicAccessBlockConfiguration={
                "BlockPublicAcls":       True,
                "IgnorePublicAcls":      True,
                "BlockPublicPolicy":     True,
                "RestrictPublicBuckets": True,
            },
        )

        # Activer le chiffrement AES-256
        s3.put_bucket_encryption(
            Bucket=BUCKET_NAME,
            ServerSideEncryptionConfiguration={
                "Rules": [{
                    "ApplyServerSideEncryptionByDefault": {
                        "SSEAlgorithm": "AES256"
                    }
                }]
            },
        )

        # Activer le versioning
        s3.put_bucket_versioning(
            Bucket=BUCKET_NAME,
            VersioningConfiguration={"Status": "Enabled"},
        )

        # Créer les dossiers logiques (préfixes)
        for prefix in ["raw/", "processed/", "logs/"]:
            s3.put_object(Bucket=BUCKET_NAME, Key=prefix)

        print(f"  ✅ Bucket créé : s3://{BUCKET_NAME}")
        print(f"     - Accès public : bloqué")
        print(f"     - Chiffrement  : AES-256")
        print(f"     - Versioning   : activé")
        print(f"     - Préfixes     : raw/, processed/, logs/")

    except s3.exceptions.BucketAlreadyOwnedByYou:
        print(f"  ℹ️  Bucket déjà existant : {BUCKET_NAME}")
    except Exception as e:
        print(f"  ❌ Erreur S3 : {e}")


# ══════════════════════════════════════════════════════════════════════════════
#  2. Security Group pour RDS
# ══════════════════════════════════════════════════════════════════════════════
def create_security_group() -> str:
    print("\n🔒 Création du Security Group RDS...")
    try:
        vpcs = ec2.describe_vpcs(Filters=[{"Name": "isDefault", "Values": ["true"]}])
        vpc_id = vpcs["Vpcs"][0]["VpcId"]

        sg = ec2.create_security_group(
            GroupName="rds-jobs-sg",
            Description="Security group for Jobs RDS PostgreSQL",
            VpcId=vpc_id,
        )
        sg_id = sg["GroupId"]

        # Autoriser uniquement le port PostgreSQL depuis cette machine
        import requests as req
        my_ip = req.get("https://api.ipify.org").text.strip()

        ec2.authorize_security_group_ingress(
            GroupId=sg_id,
            IpPermissions=[{
                "IpProtocol": "tcp",
                "FromPort":   DB_PORT,
                "ToPort":     DB_PORT,
                "IpRanges":   [{"CidrIp": f"{my_ip}/32", "Description": "My IP"}],
            }],
        )

        print(f"  ✅ Security Group créé : {sg_id}")
        print(f"     - Accès PostgreSQL autorisé depuis : {my_ip}")
        return sg_id

    except ec2.exceptions.ClientError as e:
        if "InvalidGroup.Duplicate" in str(e):
            sgs = ec2.describe_security_groups(Filters=[{"Name": "group-name", "Values": ["rds-jobs-sg"]}])
            sg_id = sgs["SecurityGroups"][0]["GroupId"]
            print(f"  ℹ️  Security Group déjà existant : {sg_id}")
            return sg_id
        print(f"  ❌ Erreur Security Group : {e}")
        return ""


# ══════════════════════════════════════════════════════════════════════════════
#  3. RDS PostgreSQL — Data Warehouse
# ══════════════════════════════════════════════════════════════════════════════
def create_rds_instance(sg_id: str):
    print("\n🗄️  Création de l'instance RDS PostgreSQL...")
    try:
        rds.create_db_instance(
            DBInstanceIdentifier = "jobs-db",
            DBInstanceClass      = "db.t3.micro",   # Free Tier
            Engine               = "postgres",
            EngineVersion        = "17.2",
            MasterUsername       = DB_USER,
            MasterUserPassword   = DB_PASSWORD,
            DBName               = DB_NAME,
            AllocatedStorage     = 20,              # Go — Free Tier max
            StorageType          = "gp2",
            PubliclyAccessible   = True,            # pour accès local
            MultiAZ              = False,
            StorageEncrypted     = True,            # chiffrement activé
            VpcSecurityGroupIds  = [sg_id],
            BackupRetentionPeriod= 0,               # pas de backup (dev)
            Tags=[
                {"Key": "Project", "Value": "jobs-data"},
                {"Key": "Env",     "Value": "dev"},
            ],
        )
        print("  ⏳ Instance RDS en cours de création (5-10 min)...")
        print("     Attente que l'instance soit disponible...")

        waiter = rds.get_waiter("db_instance_available")
        waiter.wait(DBInstanceIdentifier="jobs-db")

        # Récupère l'endpoint
        info = rds.describe_db_instances(DBInstanceIdentifier="jobs-db")
        endpoint = info["DBInstances"][0]["Endpoint"]["Address"]

        print(f"  ✅ RDS créé !")
        print(f"     - Endpoint : {endpoint}")
        print(f"     - Port     : {DB_PORT}")
        print(f"     - DB       : {DB_NAME}")
        print(f"     - User     : {DB_USER}")
        print(f"\n  👉 Ajoute dans ton .env :")
        print(f"     DB_HOST={endpoint}")

        return endpoint

    except rds.exceptions.DBInstanceAlreadyExistsFault:
        info = rds.describe_db_instances(DBInstanceIdentifier="jobs-db")
        endpoint = info["DBInstances"][0]["Endpoint"]["Address"]
        print(f"  ℹ️  RDS déjà existant → endpoint : {endpoint}")
        return endpoint
    except Exception as e:
        print(f"  ❌ Erreur RDS : {e}")
        return ""


# ══════════════════════════════════════════════════════════════════════════════
#  4. Initialisation du schéma SQL
# ══════════════════════════════════════════════════════════════════════════════
def init_database(host: str):
    print("\n🏗️  Initialisation du schéma SQL...")
    sql_path = os.path.join(os.path.dirname(__file__), "../database/create_tables.sql")

    if not os.path.exists(sql_path):
        print(f"  ⚠️  Fichier SQL introuvable : {sql_path}")
        return

    try:
        conn = psycopg2.connect(
            host=host, port=DB_PORT,
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            with open(sql_path, "r") as f:
                cur.execute(f.read())
        conn.close()
        print("  ✅ Tables créées avec succès")
    except Exception as e:
        print(f"  ❌ Erreur initialisation BDD : {e}")


# ══════════════════════════════════════════════════════════════════════════════
#  Main
# ══════════════════════════════════════════════════════════════════════════════
def main():
    print("=" * 55)
    print("  SETUP INFRASTRUCTURE AWS")
    print("=" * 55)

    create_s3_bucket()
    sg_id    = create_security_group()
    endpoint = create_rds_instance(sg_id) if sg_id else ""

    if endpoint:
        init_database(endpoint)

    print("\n" + "=" * 55)
    print("  ✅ Infrastructure prête !")
    print("=" * 55)
    print("""
Prochaines étapes :
  1. Mets à jour DB_HOST dans ton .env avec l'endpoint RDS
  2. Lance : python scraping/scrape_jobs.py
  3. Lance : python etl/transform.py
  4. Lance : python dashboard/dashboard.py
""")


if __name__ == "__main__":
    main()
