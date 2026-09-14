terraform {
  required_providers {
    kind = {
      source  = "tehcyx/kind"
      version = "~> 0.4.0"
    }
  }

  # Local environment deliberately uses a local backend for fast, dependency-free bootstrapping
  backend "local" {
    path = "terraform.tfstate"
  }
}

provider "kind" {}

resource "kind_cluster" "eventdrivenmicroservices" {
  name           = "eventdrivenmicroservices-cluster"
  wait_for_ready = true

  kind_config {
    kind        = "Cluster"
    api_version = "kind.x-k8s.io/v1alpha4"

    node {
      role = "control-plane"
      kubeadm_config_patches = [
        "kind: InitConfiguration\nnodeRegistration:\n  kubeletExtraArgs:\n    node-labels: \"ingress-ready=true\"\n"
      ]
      extra_port_mappings {
        container_port = 80
        host_port      = 80
        protocol       = "TCP"
      }
      extra_port_mappings {
        container_port = 443
        host_port      = 443
        protocol       = "TCP"
      }
    }
    
    node {
      role = "worker"
    }
  }
}
