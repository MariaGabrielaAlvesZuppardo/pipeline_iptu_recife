import os
import io
import pandas as pd
from dotenv import load_dotenv
from modules.utils.logger import setup_logger
from modules.ingestors.csv_ingestor import CSVIngestor
from modules.ingestors.json_ingestor import JSONIngestor
from modules.mappers.schema_mapper import SchemaMapper
from modules.transformers.harmonizer import Harmonizer
from modules.utils.downloader import Downloader
from modules.loaders.s3_loader import S3Loader
from modules.quality.quality_checker import QualityChecker

# Carrega variáveis do .env
load_dotenv()
logger = setup_logger("main")

urls = {
    "2020": {"url": "https://github.com/Neurolake/challenge-data-engineer/raw/40c5c92c624c3b333fe670eceedb7ca6a0213f25/iptu_20_23/iptu_2020.zip", "tipo": "csv"},
    "2021": {"url": "https://github.com/Neurolake/challenge-data-engineer/raw/40c5c92c624c3b333fe670eceedb7ca6a0213f25/iptu_20_23/iptu_2021.zip", "tipo": "csv"},
    "2022": {"url": "https://github.com/Neurolake/challenge-data-engineer/raw/40c5c92c624c3b333fe670eceedb7ca6a0213f25/iptu_20_23/iptu_2022.zip", "tipo": "csv"},
    "2023": {"url": "https://github.com/Neurolake/challenge-data-engineer/raw/40c5c92c624c3b333fe670eceedb7ca6a0213f25/iptu_20_23/iptu_2023.zip", "tipo": "csv"},
    "2024": {"url": "https://github.com/Neurolake/challenge-data-engineer/raw/40c5c92c624c3b333fe670eceedb7ca6a0213f25/iptu_24/iptu_2024_json.zip", "tipo": "json"}
}

def main():
    logger.info("Iniciando pipeline IPTU Recife 🚀")

    # Inicializar componentes
    downloader = Downloader()
    quality_checker = QualityChecker()  # ✨ QualityChecker adicionado
    mapper = SchemaMapper()
    harmonizer = Harmonizer()
    
    # Inicializar S3 Loaders para cada camada
    s3_loader_raw = S3Loader(bucket=os.getenv("S3_BUCKET_RAW", "iptu-recife-raw"))
    s3_loader_processed = S3Loader(bucket=os.getenv("S3_BUCKET_PROCESSED", "iptu-recife-processed"))
    s3_loader_quality = S3Loader(bucket=os.getenv("S3_BUCKET_QUALITY", "iptu-recife-quality"))

    all_dfs = []
    stats = {
        "total_valid": 0,
        "total_invalid": 0,
        "by_year": {}
    }

    for ano, meta in urls.items():
        logger.info(f"📥 Processando IPTU {ano} ({meta['tipo']})")

        # 1. Baixar ZIP direto para memória
        zip_buffer = downloader.download_zip_in_memory(meta["url"])
        
        # Converter BytesIO para bytes se necessário
        if isinstance(zip_buffer, io.BytesIO):
            zip_buffer.seek(0)
            zip_bytes = zip_buffer.read()
            zip_buffer.seek(0)  # Reset para uso posterior
        else:
            zip_bytes = zip_buffer
            zip_buffer = io.BytesIO(zip_bytes)
        
        logger.info(f"✓ Download concluído: {len(zip_bytes)} bytes")

        # 2. Upload dos dados RAW
        s3_key_raw = f"year={ano}/iptu_{ano}.zip"
        s3_loader_raw.upload_bytes(zip_bytes, key=s3_key_raw)
        logger.info(f"📦 {ano} raw enviado para s3://{s3_loader_raw.bucket}/{s3_key_raw}")

        # 3. Ler CSV/JSON diretamente do ZIP (in-memory)
        if meta["tipo"] == "csv":
            ingestor = CSVIngestor(zip_buffer, f"iptu_{ano}.csv")
        else:
            ingestor = JSONIngestor(zip_buffer, f"iptu_{ano}.json")
        
        df_raw = ingestor.load()
        logger.info(f"✓ {len(df_raw)} registros carregados do ano {ano}")

        # 4. 🔍 VALIDAÇÃO DE QUALIDADE NOS DADOS BRUTOS (ANTES DO PROCESSAMENTO)
        logger.info(f"🔍 Validando qualidade dos dados brutos {ano}...")
        df_valid, df_invalid = quality_checker.validate(df_raw)
        
        # Estatísticas de qualidade
        valid_count = len(df_valid)
        invalid_count = len(df_invalid)
        stats["total_valid"] += valid_count
        stats["total_invalid"] += invalid_count
        stats["by_year"][ano] = {
            "valid": valid_count,
            "invalid": invalid_count,
            "quality_rate": round((valid_count / (valid_count + invalid_count)) * 100, 2) if (valid_count + invalid_count) > 0 else 0
        }
        
        logger.info(f"✓ {valid_count} registros válidos e {invalid_count} inválidos do ano {ano}")

        # 5. Upload dos dados INVÁLIDOS para auditoria (ANTES de qualquer transformação)
        if not df_invalid.empty:
            df_invalid['ano'] = ano
            s3_key_invalid = f"year={ano}/iptu_invalid_{ano}.parquet"
            s3_loader_quality.upload_dataframe(df_invalid, key=s3_key_invalid)
            logger.info(f"⚠️ {ano} inválidos enviados para s3://{s3_loader_quality.bucket}/{s3_key_invalid}")

        # 6. Mapear schema APENAS nos dados válidos
        logger.info(f"🗺️ Mapeando schema para {ano}...")
        df_mapped = mapper.map_columns(df_valid)

        # 7. Harmonizar campos
        logger.info(f"🔧 Harmonizando dados para {ano}...")
        df_harmonized = harmonizer.run(df_mapped)

        # Adicionar coluna de ano para particionamento
        df_harmonized['ano'] = ano

        # Guardar para consolidação final
        all_dfs.append(df_harmonized)

        # 8. Upload dos dados PROCESSADOS (com particionamento)
        s3_key_valid = f"year={ano}/iptu_{ano}.parquet"
        s3_loader_processed.upload_dataframe(df_harmonized, key=s3_key_valid)
        logger.info(f"✅ {ano} processados enviados para s3://{s3_loader_processed.bucket}/{s3_key_valid}")

    # 9. Consolidação final de todos os anos
    logger.info("🔄 Consolidando dataset unificado...")
    logger.info(f"📊 Verificando DataFrames antes da consolidação:")

    # Log detalhado de cada DataFrame
    for i, df in enumerate(all_dfs):
        ano = df['ano'].iloc[0] if 'ano' in df.columns and len(df) > 0 else 'desconhecido'
        ano_ex_unique = df['ano_exercicio'].unique() if 'ano_exercicio' in df.columns else []
        logger.info(f"  DataFrame {i}: {len(df):,} registros | ano={ano} | ano_exercicio={list(ano_ex_unique)}")

    final_df = pd.concat(all_dfs, ignore_index=True)

    # Log após consolidação
    logger.info(f"✓ Consolidação concluída: {len(final_df):,} registros totais")
    logger.info(f"📊 Distribuição por ano no dataset unificado:")

    # Contar por coluna 'ano'
    if 'ano' in final_df.columns:
        contagem_ano = final_df['ano'].value_counts().sort_index()
        for ano, count in contagem_ano.items():
            logger.info(f"  ano={ano}: {count:,} registros")

    # Contar por coluna 'ano_exercicio'
    if 'ano_exercicio' in final_df.columns:
        logger.info(f"📊 Distribuição por ano_exercicio no dataset unificado:")
        contagem_ano_ex = final_df['ano_exercicio'].value_counts().sort_index()
        for ano_ex, count in contagem_ano_ex.items():
            if pd.notna(ano_ex):
                logger.info(f"  ano_exercicio={int(ano_ex)}: {count:,} registros")

    s3_key_final = "iptu_unificado.parquet"
    s3_loader_processed.upload_dataframe(final_df, key=s3_key_final)
    logger.info(f"🏁 Dataset unificado ({len(final_df)} registros) enviado para s3://{s3_loader_processed.bucket}/{s3_key_final}")
    # 10. Gerar relatório de qualidade
    logger.info("📊 Gerando relatório de qualidade...")
    quality_report = {
        "execution_date": pd.Timestamp.now().isoformat(),
        "total_records_processed": stats["total_valid"] + stats["total_invalid"],
        "total_valid_records": stats["total_valid"],
        "total_invalid_records": stats["total_invalid"],
        "overall_quality_rate": round((stats["total_valid"] / (stats["total_valid"] + stats["total_invalid"])) * 100, 2),
        "by_year": stats["by_year"]
    }
    
    # Upload do relatório de qualidade
    import json
    quality_json = json.dumps(quality_report, indent=2)
    s3_key_report = "quality_report.json"
    s3_loader_quality.upload_bytes(quality_json.encode('utf-8'), key=s3_key_report)
    logger.info(f"📈 Relatório de qualidade enviado para s3://{s3_loader_quality.bucket}/{s3_key_report}")

    # 11. Sumário final
    logger.info("=" * 60)
    logger.info("🎉 PIPELINE CONCLUÍDA COM SUCESSO!")
    logger.info("=" * 60)
    logger.info(f"Total de registros processados: {stats['total_valid'] + stats['total_invalid']:,}")
    logger.info(f"Registros válidos: {stats['total_valid']:,}")
    logger.info(f"Registros inválidos: {stats['total_invalid']:,}")
    logger.info(f"Taxa de qualidade geral: {quality_report['overall_quality_rate']}%")
    logger.info("=" * 60)
    logger.info("📦 Buckets S3:")
    logger.info(f"  Raw: s3://{s3_loader_raw.bucket}/")
    logger.info(f"  Processed: s3://{s3_loader_processed.bucket}/")
    logger.info(f"  Quality: s3://{s3_loader_quality.bucket}/")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()