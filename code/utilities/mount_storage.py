# Databricks notebook source
# MAGIC %md
# MAGIC # 스토리지 마운트/연결 유틸리티
# MAGIC > ADLS Gen2, S3 등 외부 스토리지 연결 패턴

# ============================================================
# ⚠️ 참고: Unity Catalog 환경에서는 마운트 대신
#    External Location + Volume 사용을 권장합니다.
#    아래 마운트 방식은 레거시 또는 UC 미적용 환경용입니다.
# ============================================================


# ============================================================
# 방법 1: Unity Catalog Volume (권장)
# ============================================================

# SQL로 Volume 생성 후 직접 경로 사용
# CREATE VOLUME IF NOT EXISTS catalog.schema.my_volume;
# 경로: /Volumes/catalog/schema/my_volume/file.csv

def read_from_volume(catalog, schema, volume, file_path, format="csv", **options):
    """Unity Catalog Volume에서 파일을 읽습니다."""
    full_path = f"/Volumes/{catalog}/{schema}/{volume}/{file_path}"
    reader = spark.read.format(format)
    for k, v in options.items():
        reader = reader.option(k, v)
    return reader.load(full_path)


# ============================================================
# 방법 2: ADLS Gen2 직접 접근 (Service Principal)
# ============================================================

def configure_adls_spn(storage_account, tenant_id, client_id, secret_scope, secret_key):
    """ADLS Gen2를 Service Principal로 설정합니다."""
    spark.conf.set(
        f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net",
        "OAuth"
    )
    spark.conf.set(
        f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net",
        client_id
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net",
        dbutils.secrets.get(scope=secret_scope, key=secret_key)
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",
        f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
    )
    print(f"✅ ADLS Gen2 설정 완료: {storage_account}")
    # 사용: spark.read.load("abfss://container@{storage_account}.dfs.core.windows.net/path/")


# ============================================================
# 방법 3: 레거시 마운트 (UC 미적용 환경)
# ============================================================

def mount_adls(storage_account, container, mount_point, tenant_id, client_id, secret_scope, secret_key):
    """ADLS Gen2를 DBFS에 마운트합니다."""
    if any(m.mountPoint == mount_point for m in dbutils.fs.mounts()):
        print(f"ℹ️ 이미 마운트됨: {mount_point}")
        return

    configs = {
        "fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id": client_id,
        "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope=secret_scope, key=secret_key),
        "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
    }

    dbutils.fs.mount(
        source=f"abfss://{container}@{storage_account}.dfs.core.windows.net/",
        mount_point=mount_point,
        extra_configs=configs
    )
    print(f"✅ 마운트 완료: {mount_point}")


def unmount(mount_point):
    """마운트 해제."""
    dbutils.fs.unmount(mount_point)
    print(f"✅ 마운트 해제: {mount_point}")


def list_mounts():
    """현재 마운트 목록을 출력합니다."""
    for m in dbutils.fs.mounts():
        print(f"  {m.mountPoint} → {m.source}")
