#!/bin/bash
# Run only after all required results/checkpoints have been downloaded.
# STOP DESTROYS NVMe. EBS contents survive; this does not terminate instances.
set -euo pipefail
export AWS_CONFIG_FILE='/Users/chroma/Documents/ChatGPT/Hierarchical SPANN/horizontal-sharding/fleet/aws-config'
account=$(aws --profile spann-sandbox --region us-east-1 sts get-caller-identity --query Account --output text)
[[ "$account" == '079819354890' ]] || { echo 'Refusing: wrong AWS account' >&2; exit 1; }
aws --profile spann-sandbox --region us-east-1 ec2 stop-instances --instance-ids i-09b86223b5620b35d i-011ff180f03584f40 i-00268d1e1dfea0357
