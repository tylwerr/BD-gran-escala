# Services

For the users and passwords, please check the .devcontainer/docker-compose.yml file.

## pgadmin ui 
http://localhost:8888

Configure the sever as:
Host: postgresql
Port: 5432
Username: postgres
Password: password-root

## Minio ui
http://127.0.0.1:9001/browser


## Redis ui 
http://localhost:5540

## Kafka ui
http://localhost:8080/

## Terraform 
- Create a postgresql database, user and schema
- create a kafka topic and subscriptor
- create a minio bucket 
https://registry.terraform.io/providers/aminueza/minio/latest/docs

For the first run, clean any previous execution file

``` bash
rm infra/terraform.tfstate* 
rm infra/foo.bar2 
rm -rf infra/.terraform*
```

## Run terraform
``` bash
cd infra
# init the env
terraform init
# run a plan to validate the changes
terraform plan
# apply changes
terraform apply
```

### Optional: run with variables
``` bash
terraform plan --file-name dev.tfvars
terraform plan --file-name prod.tfvars 
```

# Python app: data generador

## Print in terminal

`python src/random_data_generator.py `


Create 2 records and show in terminal

`python src/random_data_generator.py --num_records 2`

## Store in local file

`python src/random_data_generator.py --output file_path --output_path output/data.json --num_records 10 `


## Store in a file in a bucket minio

`python src/random_data_generator.py --output minio --output_path output.json --num_records 2 `

## Sent to a Kafka topic

`python src/random_data_generator.py --output minio --output_path output.json --num_records 2 `


