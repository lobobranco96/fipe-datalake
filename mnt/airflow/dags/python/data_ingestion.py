from google.cloud import storage
import os

def upload_files_to_gcs(bucket_name, source_folder, destination_folder):
    # Cria um cliente de armazenamento
    key_path = "/opt/airflow/dags/credential/google_credential.json"

# Cria um cliente com credenciais específicas
    client = storage.Client.from_service_account_json(key_path)
    #client = storage.Client()

    # Referencia o bucket do GCS
    bucket = client.get_bucket(bucket_name)

    # Itera sobre todos os arquivos no diretório local
    for root, dirs, files in os.walk(source_folder):
        for file_name in files:
            local_file_path = os.path.join(root, file_name)
            # Define o caminho no GCS
            gcs_file_path = os.path.join(destination_folder, file_name)

            # # Faz o upload para o GCS
            blob = bucket.blob(gcs_file_path)
            blob.upload_from_filename(local_file_path)

            print(f'Arquivo {file_name} enviado para {gcs_file_path}')

# Envia os arquivos
if __name__ == "__main__":
    upload_files_to_gcs(bucket_name, source_folder, destination_folder)