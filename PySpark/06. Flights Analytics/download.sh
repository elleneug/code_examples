#aws --profile=karpov-user --endpoint-url=https://storage.yandexcloud.net s3 cp s3://ny-taxi-data/ny-taxi/yellow_tripdata_2020-04.csv ./data/


mkdir ~/.mysql && \                                                                                                        [2]
wget "https://storage.yandexcloud.net/cloud-certs/CA.pem" -O ~/.mysql/root.crt && \
chmod 0600 ~/.mysql/root.crt

wget -O ./jars/mysql-connector-java-8.0.25.jar https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.25/mysql-connector-java-8.0.25.jar

