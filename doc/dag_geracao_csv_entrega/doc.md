## DAG Documentation 
Essa DAG envia os arquivos de ponteiro capturados do RDS de Entrega 
para o Snowflake, para realizar a atualização dos ponteiros "ANTES" 
da virada.
Também envia os arquivos CSV para o AWS S3 e, na sequencia,
chama as procedures do RDS para carregar esses arquivos.