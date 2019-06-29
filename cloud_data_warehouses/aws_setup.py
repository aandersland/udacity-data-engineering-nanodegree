import pandas as pd
import boto3
import json
import configparser
from botocore.exceptions import ClientError


config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')

REGION = 'us-east-1'

DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
DWH_DB = config.get("DWH", "DWH_DB")
DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")
DWH_PORT = config.get("DWH", "DWH_PORT")

DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")

# (DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB)

pd.DataFrame({"Param":
                  ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE",
                   "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER",
                   "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
              "Value":
                  [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE,
                   DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD,
                   DWH_PORT, DWH_IAM_ROLE_NAME]
              })


def create_aws_client(_type, _region, _key, _secret):
    """
    Method to create an AWS client/resource
    :param _type: Type of client/resource to create (ec2, s3, iam, redshift)
    :param _region: AWS region
    :param _key: AWS user key
    :param _secret: AWS user secret
    :return: A client or resource
    """
    _res = boto3.resource(_type,
                          region_name=_region,
                          aws_access_key_id=_key,
                          aws_secret_access_key=_secret
                          )
    return _res


def get_s3_data(_bucket, _prefix):
    """
    Method to get s3 data
    :param _bucket: S3 bucket
    :param _prefix: Prefix to filter data
    """
    s3_bucket = s3.Bucket(_bucket)
    for obj in s3_bucket.objects.filter(Prefix=_prefix):
        print(obj)


def create_aws_iam_role(_role_name, _description):
    """
    Method to create an iam role in AWS
    :param _role_name: Name of role to be created.
    :param _description: Description of the role name
    """
    # 1.1 Create the role,
    try:
        print("1.1 Creating a new IAM Role")
        dwh_role = iam.create_role(
            Path='/',
            RoleName=_role_name,
            Description="Allows Redshift clusters to call AWS services on your "
                        "behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                                'Effect': 'Allow',
                                'Principal': {
                                    'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )
    except Exception as e:
        print(e)


def add_aws_role_policy(_role_name, _policy_arn):
    """
    Method to add a role to an AWS policy
    :param _role_name: Role name to be added
    :param _policy_arn: Policy ARN
    """
    print("1.2 Attaching Policy")
    iam.attach_role_policy(RoleName=_role_name, PolicyArn=_policy_arn)['ResponseMetadata']['HTTPStatusCode']


def get_aws_role_arn(_role_name):
    """
    Method to return the AWS ARN from a role.
    :param _role_name: Roll name
    :return: ARN for the provided role
    """
    print("1.3 Get the IAM role ARN")
    _role_arn = iam.get_role(RoleName=_role_name)['Role']['Arn']
    return _role_arn


def create_redshift_cluster(_cluster_type, _node_type, _num_nodes,
                            _cluster_identifier, _db_name, _db_user,
                            _db_password, _role_arn):
    _response = ''
    try:
        _response = redshift.create_cluster(
            # HW
            ClusterType=_cluster_type,
            NodeType=_node_type,
            NumberOfNodes=int(_num_nodes),

            # Identifiers & Credentials
            DBName=_db_name,
            ClusterIdentifier=_cluster_identifier,
            MasterUsername=_db_name,
            MasterUserPassword=_db_password,

            # Roles (for s3 access)
            IamRoles=[_role_arn]
        )
    except Exception as e:
        print(e)
    return _response


def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


def get_aws_cluster_endpoint():
    DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
    print("DWH_ENDPOINT :: ", endpoint)
    return DWH_ENDPOINT


def get_aws_cluster_role_arn():
    DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
    print("DWH_ROLE_ARN :: ", roleArn)
    return DWH_ROLE_ARN


def open_aws_tcp_port_to_cluster():
    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print(e)


def delete_aws_redshift_cluster():
    redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
                            SkipFinalClusterSnapshot=True)


def delete_aws_iam_policy():
    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)


def delete_aws_iam_role():
    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)


def get_aws_cluster_properties():
    myClusterProps = \
    redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)[
        'Clusters'][0]
    prettyRedshiftProps(myClusterProps)

# ------------------------------------------------------------------------------
ec2 = create_aws_client('ec2', REGION, KEY, SECRET)
s3 = create_aws_client('s3', REGION, KEY, SECRET)
iam = create_aws_client('iam', REGION, KEY, SECRET)
redshift = create_aws_client('redshift', REGION, KEY, SECRET)

get_s3_data('awssampledbuswest2', 'ssbgz')
create_aws_iam_role(DWH_IAM_ROLE_NAME, "Allows Redshift clusters to call AWS "
                                       "services on your behalf.")
add_aws_role_policy()
role_arn = get_aws_role_arn()
print(role_arn)


get_aws_cluster_properties()

open_aws_tcp_port_to_cluster()

conn_string="postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT,DWH_DB)
print(conn_string)

get_aws_cluster_properties()
