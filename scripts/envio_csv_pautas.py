import boto3
import sys

arquivolocal = ""
arquivos3 = ""
if len(sys.argv)  >= 1:
	arquivolocal = sys.argv[1]
if len(sys.argv)  >= 2:
	arquivos3 = sys.argv[2]

# ===============================================
# CONFIGURAÇÕES
# ===============================================

ROLE_1_ARN = "arn:aws:iam::939357810902:role/RoleS3VertexPautasCSV"
ROLE_2_ARN = "arn:aws:iam::954235624237:role/sandbox-trm-pauta-content-s3-systax-us-east-2"

SESSION_NAME_ROLE1 = "upload-session"
SESSION_NAME_ROLE2 = "upload-session"

LOCAL_FILE = arquivolocal
BUCKET_NAME = "sandbox-trm-pauta-content-us-east-2"
BUCKET_NAME = "systaxlinks"
OBJECT_KEY = arquivos3

BASE_PROFILE = "systax"
# ===============================================
# FUNÇÃO PARA ASSUMIR UMA ROLE COM CREDENCIAIS ESPECÍFICAS
# ===============================================

def assume_role(role_arn, session_name, base_session=None):
    """
    base_session = boto3.Session(...) com credenciais já assumidas
                 = None -> usar credenciais padrão (~/.aws/credentials)
    """
    try:
        if base_session:
            sts = base_session.client("sts")
        else:
            base_session = boto3.Session(profile_name=BASE_PROFILE)
            sts = base_session.client("sts")

        print(f"\nAssumindo role: {role_arn} ...")

        response = sts.assume_role(
            RoleArn=role_arn,
            RoleSessionName=session_name
        )

        creds = response["Credentials"]

        # Retorna uma nova sessão usando as credenciais temporárias
        return boto3.Session(
            aws_access_key_id     = creds["AccessKeyId"],
            aws_secret_access_key = creds["SecretAccessKey"],
            aws_session_token     = creds["SessionToken"]
        )
    except Exception as e:
        print("\n❌ ERRO ao assumir a role:")
        print(f"Role ARN: {role_arn}")
        print(f"Mensagem completa da AWS:\n{e}\n")
        print(boto3.Session(session_name).get_credentials().get_frozen_credentials())
        raise


# ===============================================
# 1. ASSUME ROLE 1
# ===============================================

session_role1 = assume_role(
    role_arn=ROLE_1_ARN,
    session_name=SESSION_NAME_ROLE1,
    base_session=None  # usa credenciais padrão da máquina
)
print(session_role1)
print("Role 1 assumida com sucesso!")
'''
# ===============================================
# 2. ASSUME ROLE 2 (USANDO CREDENCIAIS DA ROLE 1)
# ===============================================
session_role2 = assume_role(
    role_arn=ROLE_2_ARN,
    session_name=SESSION_NAME_ROLE2,
    base_session=session_role1
)
print("Role 2 assumida com sucesso!")
'''
# ===============================================
# 3. UPLOAD PARA O S3 USANDO A SEGUNDA ROLE
# ===============================================

print(f"\nRealizando upload: {LOCAL_FILE} -> s3://{BUCKET_NAME}/{OBJECT_KEY}")

s3 = session_role1.client("s3")
#s3 = session_role2.client("s3")

s3.upload_file(LOCAL_FILE, BUCKET_NAME, OBJECT_KEY)

print("\nUpload concluído com sucesso!")
