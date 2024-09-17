# iteso-bdnr-cassandra

A place to share cassandra app code

### Setup a python virtual env with python cassandra installed
```
# If pip is not present in you system
sudo apt update
sudo apt install python3-pip

# Install and activate virtual env (Linux/MacOS)
python3 -m pip install virtualenv
python3 -m venv ./venv
source ./venv/bin/activate

# Install and activate virtual env (Windows)
python3 -m pip install virtualenv
python3 -m venv ./venv
.\venv\Scripts\Activate.ps1

# Install project python requirements
pip install -r requirements.txt
```


### Launch cassandra container
```
# To start a new container
docker run --name node01 -p 9042:9042 -d cassandra

# If container already exists just start it
docker start node01
```

### Start a Cassandra cluster with 2 nodes
```
# Recipe to create a cassandra cluster using docker
docker run --name node01 -p 9042:9042 -d cassandra
docker run --name node02 -d --link node01:cassandra cassandra

# Wait for containers to be fully initialized, verify node status
docker exec -it node01 nodetool status
```
