# Common Parameters
common_params = {
  project = "daring-fiber-439305-v4" # Replace with your GCP project ID
  region  = "asia-south1" # Replace with your desired GCP region
}

# Dataflow parameters
dataflow_params = {
  template_params = {
    instance_id             = "spanner-reverse-replication" # Spanner instance ID
    database_id             = "ecommerce" # Spanner database ID
    local_session_file_path = "session.json"
    source_type             = "cassandra"
  }
  runner_params = {
    max_workers      = "1"         # Maximum number of worker VMs
    num_workers      = "1"         # Initial number of worker VMs
    machine_type     = "n2-standard-2"        # Machine type for worker VMs (e.g., "n2-standard-2")
    network          = "reverse-replication"         # VPC network for the Dataflow job
    subnetwork       = "reverse-replication-asia-south1" # Give the full path to the subnetwork
    ip_configuration = "WORKER_IP_PRIVATE"
    sdk_container_image = "gcr.io/daring-fiber-439305-v4/templates/spanner-to-sourcedb:latest"
  }
}

shard_config = {
  host             = "10.0.0.2"
  port             = "9402"
  username         = "ollion"
  password         = "Ollion_2023"
  keyspace         = "ecommerce"
  consistencyLevel = "LOCAL_QUORUM"
  sslOptions       = false
  protocolVersion  = "v5"
  dataCenter       = "datacenter1"
  localPoolSize    = "2"
  remotePoolSize   = "1"
}

cassandra_template_config_file = "./cassandra-config-template.conf"