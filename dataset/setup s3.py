import boto3
import os
import sys
from botocore.exceptions import ClientError, NoCredentialsError


BUCKET_NAME = "streamify-s3"          
AWS_REGION  = "us-east-1"           

AWS_ACCESS_KEY_ID     = ""   
AWS_SECRET_ACCESS_KEY = ""   

CSV_DIR = os.path.dirname(os.path.abspath(__file__))


S3_FOLDERS = [
    "raw-data/user-profiles/",
    "raw-data/content-catalog/",
    "raw-data/user-interactions/",
    "raw-data/subscription-history/",
    "processed-data/user_content_matrix/",
    "processed-data/content_stats/",
    "processed-data/user_features/",
    "scripts/",
    "logs/",
    "models/",
    "temp/",
]


CSV_TO_S3_PATH = {
    "user_profiles.csv":       "raw-data/user-profiles/user_profiles.csv",
    "content_catalog.csv":     "raw-data/content-catalog/content_catalog.csv",
    "user_interactions.csv":   "raw-data/user-interactions/user_interactions.csv",
    "subscription_history.csv":"raw-data/subscription-history/subscription_history.csv",
}



def get_s3_client():
    """Buat S3 client. Prioritas: hardcode → env var → AWS CLI profile."""
    kwargs = {"region_name": AWS_REGION}
    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        kwargs["aws_access_key_id"]     = AWS_ACCESS_KEY_ID
        kwargs["aws_secret_access_key"] = AWS_SECRET_ACCESS_KEY
        print("Menggunakan kredensial hardcode.")
    else:
        print("Menggunakan kredensial dari environment variable / AWS CLI profile.")
    return boto3.client("s3", **kwargs)


def create_bucket(s3, bucket_name, region):
    """Buat bucket jika belum ada."""
    try:
        if region == "us-east-1":
            s3.create_bucket(Bucket=bucket_name)
        else:
            s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region}
            )
        print(f"  [OK] Bucket '{bucket_name}' berhasil dibuat.")
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            print(f"  [--] Bucket '{bucket_name}' sudah ada, lanjut.")
        else:
            raise


def create_folders(s3, bucket_name, folders):
    """Buat folder (placeholder kosong) di S3."""
    print(f"\nMembuat struktur folder di s3://{bucket_name}/")
    for folder in folders:
        s3.put_object(Bucket=bucket_name, Key=folder, Body=b"")
        print(f"  [OK] {folder}")


def upload_csv_files(s3, bucket_name, csv_dir, csv_map):
    """Upload file CSV ke subfolder raw-data yang sesuai."""
    print(f"\nMengupload file CSV dari: {csv_dir}")
    success, skip, fail = 0, 0, 0

    for filename, s3_key in csv_map.items():
        local_path = os.path.join(csv_dir, filename)
        if not os.path.isfile(local_path):
            print(f"  [SKIP] {filename} tidak ditemukan di {csv_dir}")
            skip += 1
            continue
        try:
            file_size = os.path.getsize(local_path)
            s3.upload_file(local_path, bucket_name, s3_key)
            print(f"  [OK] {filename} → s3://{bucket_name}/{s3_key}  ({file_size/1024:.1f} KB)")
            success += 1
        except ClientError as e:
            print(f"  [FAIL] {filename}: {e}")
            fail += 1

    print(f"\nHasil upload: {success} berhasil | {skip} dilewati | {fail} gagal")


def print_summary(bucket_name, folders, csv_map):
    """Tampilkan ringkasan struktur S3 akhir."""
    print(f"\n{'='*55}")
    print(f"STRUKTUR S3 FINAL: s3://{bucket_name}/")
    print(f"{'='*55}")
    tree = {}
    for f in folders:
        parts = f.strip("/").split("/")
        tree.setdefault(parts[0], [])
        if len(parts) > 1:
            tree[parts[0]].append(parts[1])

    for root, subs in sorted(tree.items()):
        print(f"├── {root}/")
        for sub in subs:
            print(f"│   └── {sub}/")
        if not subs:
            pass  # folder root tanpa sub

    print(f"\nFile CSV yang diupload:")
    for fname, s3key in csv_map.items():
        print(f"  s3://{bucket_name}/{s3key}")




def main():
    print("=" * 55)
    print("  AWS S3 — Setup Bucket & Upload Dataset")
    print("=" * 55)

    # Validasi nama bucket
    if BUCKET_NAME == "nama-bucket-kamu":
        print("\n[ERROR] Harap ganti variabel BUCKET_NAME di bagian atas script!")
        sys.exit(1)

    try:
        s3 = get_s3_client()

        # 1. Buat bucket
        print(f"\nMembuat bucket: {BUCKET_NAME} (region: {AWS_REGION})")
        create_bucket(s3, BUCKET_NAME, AWS_REGION)

        # 2. Buat semua folder
        create_folders(s3, BUCKET_NAME, S3_FOLDERS)

        # 3. Upload file CSV
        upload_csv_files(s3, BUCKET_NAME, CSV_DIR, CSV_TO_S3_PATH)

        # 4. Ringkasan
        print_summary(BUCKET_NAME, S3_FOLDERS, CSV_TO_S3_PATH)

        print(f"\n[SELESAI] Setup S3 berhasil!")

    except NoCredentialsError:
        print("\n[ERROR] Kredensial AWS tidak ditemukan.")
        print("Pilihan:")
        print("  1. Isi AWS_ACCESS_KEY_ID & AWS_SECRET_ACCESS_KEY di atas script")
        print("  2. Set environment variable: export AWS_ACCESS_KEY_ID=xxx")
        print("  3. Jalankan: aws configure")
        sys.exit(1)
    except ClientError as e:
        print(f"\n[ERROR] AWS ClientError: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()