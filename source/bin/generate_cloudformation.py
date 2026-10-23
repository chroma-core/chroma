import boto3
import json
import subprocess
import os
import re


def b64text(txt):
    """Generate Base 64 encoded CF json for a multiline string, subbing in values where appropriate"""
    lines = []
    for line in txt.splitlines(True):
        if "${" in line:
            lines.append({"Fn::Sub": line})
        else:
            lines.append(line)
    return {"Fn::Base64": {"Fn::Join": ["", lines]}}


path = os.path.dirname(os.path.realpath(__file__))
version = subprocess.check_output(f"{path}/version").decode("ascii").strip()

with open(f"{path}/templates/docker-compose.yml") as f:
    docker_compose_file = str(f.read())


cloud_config_script = """
#cloud-config
cloud_final_modules:
- [scripts-user, always]
"""

cloud_init_script = f"""
#!/bin/bash
amazon-linux-extras install docker
usermod -a -G docker ec2-user
curl -L https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m) -o /usr/local/bin/docker-compose
chmod +x /usr/local/bin/docker-compose
ln -s /usr/local/bin/docker-compose /usr/bin/docker-compose
systemctl enable docker
systemctl start docker

cat << EOF > /home/ec2-user/docker-compose.yml
{docker_compose_file}
EOF

mkdir /home/ec2-user/config

docker-compose -f /home/ec2-user/docker-compose.yml up -d
"""

userdata = f"""Content-Type: multipart/mixed; boundary="//"
MIME-Version: 1.0

--//
Content-Type: text/cloud-config; charset="us-ascii"
MIME-Version: 1.0
Content-Transfer-Encoding: 7bit
Content-Disposition: attachment; filename="cloud-config.txt"

{cloud_config_script}

--//
Content-Type: text/x-shellscript; charset="us-ascii"
MIME-Version: 1.0
Content-Transfer-Encoding: 7bit
Content-Disposition: attachment; filename="userdata.txt"

{cloud_init_script}
--//--
"""

cf = {
    "AWSTemplateFormatVersion": "2010-09-09",
    "Description": "Create a stack that runs Chroma hosted on a single instance",
    "Parameters": {
        "KeyName": {
            "Description": "Name of an existing EC2 KeyPair to enable SSH access to the instance",
            "Type": "String",
            "ConstraintDescription": "If present, must be the name of an existing EC2 KeyPair.",
            "Default": "",
        },
        "InstanceType": {
            "Description": "EC2 instance type",
            "Type": "String",
            "Default": "t3.small",
        },
        "ChromaVersion": {
            "Description": "Chroma version to install",
            "Type": "String",
            "Default": version,
        },
    },
    "Conditions": {
        "HasKeyName": {"Fn::Not": [{"Fn::Equals": [{"Ref": "KeyName"}, ""]}]},
    },
    "Resources": {
        "ChromaInstance": {
            "Type": "AWS::EC2::Instance",
            "Properties": {
                "ImageId": {
                    "Fn::FindInMap": ["Region2AMI", {"Ref": "AWS::Region"}, "AMI"]
                },
                "InstanceType": {"Ref": "InstanceType"},
                "UserData": b64text(userdata),
                "SecurityGroupIds": [{"Ref": "ChromaInstanceSecurityGroup"}],
                "KeyName": {
                    "Fn::If": [
                        "HasKeyName",
                        {"Ref": "KeyName"},
                        {"Ref": "AWS::NoValue"},
                    ]
                },
                "BlockDeviceMappings": [
                    {
                        "DeviceName": {
                            "Fn::FindInMap": [
                                "Region2AMI",
                                {"Ref": "AWS::Region"},
                                "RootDeviceName",
                            ]
                        },
                        "Ebs": {"VolumeSize": 24},
                    }
                ],
            },
        },
        "ChromaInstanceSecurityGroup": {
            "Type": "AWS::EC2::SecurityGroup",
            "Properties": {
                "GroupDescription": "Chroma Instance Security Group",
                "SecurityGroupIngress": [
                    {
                        "IpProtocol": "tcp",
                        "FromPort": "22",
                        "ToPort": "22",
                        "CidrIp": "0.0.0.0/0",
                    },
                    {
                        "IpProtocol": "tcp",
                        "FromPort": "8000",
                        "ToPort": "8000",
                        "CidrIp": "0.0.0.0/0",
                    },
                ],
            },
        },
    },
    "Outputs": {
        "ServerIp": {
            "Description": "IP address of the Chroma server",
            "Value": {"Fn::GetAtt": ["ChromaInstance", "PublicIp"]},
        }
    },
    "Mappings": {"Region2AMI": {}},
}

# Populate the Region2AMI mappings
regions = boto3.client("ec2", region_name="us-east-1").describe_regions()["Regions"]
for region in regions:
    region_name = region["RegionName"]
    ami_result = boto3.client("ec2", region_name=region_name).describe_images(
        Owners=["137112412989"],
        Filters=[
            {"Name": "name", "Values": ["amzn2-ami-kernel-5.10-hvm-*-x86_64-gp2"]},
            {"Name": "root-device-type", "Values": ["ebs"]},
            {"Name": "virtualization-type", "Values": ["hvm"]},
        ],
    )
    img = ami_result["Images"][0]
    ami_id = img["ImageId"]
    root_device_name = img["BlockDeviceMappings"][0]["DeviceName"]
    cf["Mappings"]["Region2AMI"][region_name] = {
        "AMI": ami_id,
        "RootDeviceName": root_device_name,
    }


# Write the CF json to a file
json.dump(cf, open("/tmp/chroma.cf.json", "w"), indent=4)

# upload to S3
s3 = boto3.client("s3", region_name="us-east-1")
s3.upload_file(
    "/tmp/chroma.cf.json",
    "public.trychroma.com",
    f"cloudformation/{version}/chroma.cf.json",
)

# Upload to s3 under /latest version only if this is a release
pattern = re.compile(r"^\d+\.\d+\.\d+$")
if pattern.match(version):
    s3.upload_file(
        "/tmp/chroma.cf.json",
        "public.trychroma.com",
        "cloudformation/latest/chroma.cf.json",
    )
else:
    print(f"Version {version} is not a 3-part semver, not uploading to /latest")
