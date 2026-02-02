# Student Airflow Template

This is a ready-to-run Apache Airflow + Docker environment designed for classroom use. Students can use this to run Airflow DAGs that connect to MongoDB and process stock data.

## Before You Start

1. Clone the repo or click "Use this template"
2. Open the cloned repo in VS Code

3. Run the airflow-core-fernet-key.py script to generate a fernet key. This key is used to encrypt sensitive data in Airflow, such as passwords and connection strings. You can run this script in your terminal or command prompt.

you need to install the `cryptography` library if you don't have it already. You can do this by running:
```bash
pip install cryptography paramiko==3.5.1 python-dotenv sshtunnel==0.4.0
```
Note: this includes all the other libraries needed for the scripts in the Test folder to run.

Then, run the script:
```bash
python airflow-core-fernet-key.py
```

4. Copy the generated fernet key and paste it into the `editme.env` file in the `FERNET_KEY` variable. Then rename the file to just `.env` (remove the `editme` part).

5. Make sure you have Docker and Docker Compose installed on your machine. You can download them from the official Docker website. Here is the link: https://docs.docker.com/get-docker/

6. Generate SSH Keys for Snowflake Connection (windows). Run the following commands in a git-bash shell. Update the `docker-compose.yaml` file `line 85` with the path to your private key. You only have to update your user name in the path that already exists there. (Windows users do not run in powershell, use git-bash only) Provide the public key to your Snowflake admin (your teacher) to set up the key pair authentication. [Snowflake Documentation on Key Pair Auth](https://docs.snowflake.com/en/user-guide/key-pair-auth)

```bash
mkdir -p ~/.ssh
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out ~/.ssh/dbt_key.p8 -nocrypt
openssl rsa -in ~/.ssh/dbt_key.p8 -pubout -out ~/.ssh/dbt_key.pub
cat ~/.ssh/dbt_key.pub
```

6. Generate SSH Keys for Snowflake Connection (mac). Run the following commands in a terminal. Update the `docker-compose.yaml` file `line 85` with the path to your private key. You only have to update your user name in the path that already exists there. Provide the public key to your Snowflake admin (your teacher) to set up the key pair authentication.

```bash
mkdir -p ~/.ssh
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out ~/.ssh/dbt_key.p8 -nocrypt
openssl rsa -in ~/.ssh/dbt_key.p8 -pubout -out ~/.ssh/dbt_key.pub
cat ~/.ssh/dbt_key.pub | pbcopy
```

## ✅ Getting Airflow Started

1. In that VS Code Terminal Run:

### OpenMeteo Library Note (API Template)
`requirements.txt` installs `openmeteopy` via a Git URL for the API template. If the build fails or the package is unavailable, comment out the `git+https://...openmeteopy` line and rebuild the containers. The API template will automatically fall back to the vendored copy in `dags/libs/openmeteopy`.

Note: you only run the --build flag the first time or if you change something in the Dockerfile or requirements.txt After that you can just run `docker compose up -d`
```bash
docker compose up --build -d
```

2. Open [http://localhost:8080](http://localhost:8080)

Login with:
- **Username:** `airflow`
- **Password:** `airflow`

## shut down
```bash
docker compose down
```

<!-- https://airflow.apache.org/docs/apache-airflow/stable/tutorial/index.html -->
<!-- https://airflow.apache.org/docs/apache-airflow/stable/tutorial/fundamentals.html -->
<!-- https://www.youtube.com/watch?v=ouERCRRvkFQ -->
<!-- https://www.youtube.com/watch?v=RXWYPZ3T9ys -->

## Note
### Stop everything and remove containers + volumes for this project
``` bash
docker compose down --volumes --remove-orphans
```

This will stop all running containers, remove the containers, and delete any associated volumes for this project.
